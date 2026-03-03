import os
import logging
import secrets
from functools import wraps
from copy import deepcopy
import re

from dotenv import load_dotenv
from flask import Flask, flash, g, jsonify, redirect, render_template, request, session, url_for
from PIL import Image

from brick_analyzer import BrickImageAnalyzer, SUPPORTED_EXTS
from lib.mongo import get_db_name, get_mongo_uri
from lib.llm_service import get_llm_service
from lib.blob_storage import BlobStorage
from lib.image_store import load_image
from lib.compare_analysis import (
    build_pricing_summary,
    compute_colour_stats,
    compute_texture_stats,
    delta_e76,
    describe_colour_delta,
    make_difference_heatmap_overlay_png,
    png_bytes_to_data_url,
    render_defects_overlay_png,
    rgb_hex,
)

                                                                    
load_dotenv(interpolate=False)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("brickai")


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")

    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY") or os.getenv("FLASK_SECRET_KEY") or secrets.token_hex(32)
    app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_MB", "25")) * 1024 * 1024
    app.config["ALLOWED_EXTENSIONS"] = {ext.lstrip(".").lower() for ext in SUPPORTED_EXTS}

    upload_folder = os.getenv("UPLOAD_FOLDER", "uploads")
    if not os.path.isabs(upload_folder):
        upload_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), upload_folder)
    os.makedirs(upload_folder, exist_ok=True)
    app.config["UPLOAD_FOLDER"] = upload_folder

    blob = BlobStorage()
    app.extensions["blob"] = blob
    app.config["UPLOADS_CONTAINER"] = os.getenv("AZURE_BLOB_UPLOADS_CONTAINER", "uploads")
    app.config["TMP_CONTAINER"] = os.getenv("AZURE_BLOB_TMP_CONTAINER", "tmp")

    analyzer = None
    mongo_uri = None
    db_name = None
    mongo_error = None
    try:
        mongo_uri = get_mongo_uri()
        db_name = get_db_name()
        analyzer = BrickImageAnalyzer(mongo_uri=mongo_uri, db_name=db_name)
        try:
            analyzer.ensure_indexes()
        except Exception as e:
            logger.warning(f"Could not ensure brick indexes: {e}")
    except Exception as e:
        mongo_error = str(e)
        logger.warning(f"Mongo/analyzer not initialized: {e}")
    app.extensions["analyzer"] = analyzer
    app.extensions["mongo_error"] = mongo_error

               
    from ai.brick_assistant import BrickAssistant
    from ai.chat import chat_bp, init_chat

    init_chat(
        BrickAssistant(
            get_llm_service(),
            analyzer=analyzer,
            blob=blob,
            uploads_container=app.config["UPLOADS_CONTAINER"],
            upload_folder=app.config["UPLOAD_FOLDER"],
        )
    )
    app.register_blueprint(chat_bp)

                                    
    from lib.user_service import UserService

    users = None
    if mongo_uri and db_name:
        users = UserService(mongo_uri=mongo_uri, db_name=db_name)
        try:
            users.ensure_indexes()
        except Exception as e:
            logger.warning(f"Could not ensure user indexes: {e}")

                                                             
        admin_username = os.getenv("ADMIN_USERNAME")
        admin_email = os.getenv("ADMIN_EMAIL")
        admin_password = os.getenv("ADMIN_PASSWORD")
        admin_bu_code = os.getenv("ADMIN_BU_CODE")
        if admin_username and admin_email and admin_password:
            try:
                created_id = users.ensure_admin_from_env(
                    username=admin_username,
                    email=admin_email,
                    password=admin_password,
                    bu_code=admin_bu_code,
                )
                if created_id:
                    logger.info(f"Bootstrapped admin user: {admin_username}")
            except Exception as e:
                logger.warning(f"Admin bootstrap failed: {e}")

    def login_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if users is None:
                flash("App is not configured (database not connected).")
                return redirect(url_for("index"))

            user_id = session.get("user_id")
            if not user_id:
                return redirect(url_for("login"))
            user = users.get_user_by_id(user_id)
            if not user:
                session.clear()
                return redirect(url_for("login"))
            g.user = user
            g.user_bu_code = user.get("bu_code")
            return fn(*args, **kwargs)

        return wrapper

    def _is_admin(user: dict | None) -> bool:
        if not user:
            return False
        roles = user.get("roles") or []
        return isinstance(roles, list) and ("admin" in roles)

    @app.before_request
    def _load_user_into_g():
        g.user = None
        g.user_bu_code = None
        g.user_is_admin = False
        if users is None:
            return
        user_id = session.get("user_id")
        if not user_id:
            return
        try:
            u = users.get_user_by_id(user_id)
        except Exception:
            return
        if not u:
            return
        g.user = u
        g.user_bu_code = u.get("bu_code")
        g.user_is_admin = _is_admin(u)

    def admin_required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if users is None:
                flash("App is not configured (database not connected).")
                return redirect(url_for("index"))
            if not session.get("user_id"):
                return redirect(url_for("login"))
            if not getattr(g, "user_is_admin", False):
                flash("Admin access required.")
                return redirect(url_for("index"))
            return fn(*args, **kwargs)

        return wrapper

    @app.route("/", methods=["GET"], endpoint="index")
    def index():
        return render_template("index.html")

    @app.route("/health", methods=["GET"])
    def health():
        if analyzer is None:
            return {"ok": False, "error": app.extensions.get("mongo_error")}, 503
        stats = analyzer.get_database_stats()
        return {"ok": True, "db": stats, "blob": {"configured": blob.is_configured()}}

    @app.route("/uploads/<path:relpath>", methods=["GET"])
    def uploads(relpath: str):
                                                                                          
        from flask import Response, send_from_directory

        try:
            local_path = os.path.join(app.config["UPLOAD_FOLDER"], relpath)
            if os.path.exists(local_path):
                return send_from_directory(app.config["UPLOAD_FOLDER"], relpath)
        except Exception:
            pass

        if blob.is_configured():
            data = blob.download_bytes(container=app.config["UPLOADS_CONTAINER"], blob_name=relpath)

            mimetype = "application/octet-stream"
            lower = relpath.lower()
            if lower.endswith(".png"):
                mimetype = "image/png"
            elif lower.endswith(".jpg") or lower.endswith(".jpeg"):
                mimetype = "image/jpeg"
            elif lower.endswith(".webp"):
                mimetype = "image/webp"

            return Response(data, mimetype=mimetype)

        return send_from_directory(app.config["UPLOAD_FOLDER"], relpath)

    @app.route("/login", methods=["GET", "POST"], endpoint="login")
    def login():
        if users is None:
            flash("Database is not configured.")
            return render_template("login.html"), 503
        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            password = request.form.get("password") or ""
            user = users.authenticate(username, password)
            if not user:
                flash("Invalid username or password")
                return render_template("login.html"), 401
            session["user_id"] = user["_id"]
            return redirect(url_for("profile"))
        return render_template("login.html")

    @app.route("/logout", methods=["GET"], endpoint="logout")
    def logout():
        session.clear()
        return redirect(url_for("index"))

    @app.route("/register", methods=["GET", "POST"], endpoint="register")
    def register():
        if users is None:
            flash("Database is not configured.")
            return render_template("register.html"), 503
        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            email = (request.form.get("email") or "").strip()
            password = request.form.get("password") or ""
            bu_code = (request.form.get("bu_code") or "").strip() or None

            user_id = users.create_user(username=username, email=email, password=password, bu_code=bu_code)
            if not user_id:
                flash("Username or email already exists")
                return render_template("register.html"), 400
            session["user_id"] = user_id
            return redirect(url_for("profile"))
        return render_template("register.html")

    @app.route("/profile", methods=["GET", "POST"], endpoint="profile")
    @login_required
    def profile():
        user = g.user
        from lib.bu_locations_static import get_bu_location, list_bu_locations

        bu_locations = list_bu_locations()
        if request.method == "POST":
            bu_code = (request.form.get("bu_code") or "").strip() or None

                                                                           
            if bu_code and not get_bu_location(bu_code):
                flash("Please select a BU from the list.")
                return render_template("profile.html", user=user, bu_locations=bu_locations), 400

            users.update_user(user["_id"], {"bu_code": bu_code})
            return redirect(url_for("profile"))

        return render_template("profile.html", user=user, bu_locations=bu_locations)

    @app.route("/profile/avatar", methods=["POST"], endpoint="profile_avatar")
    @login_required
    def profile_avatar():
        user = g.user
        f = request.files.get("avatar")
        if not f or not f.filename:
            flash("No file selected")
            return redirect(url_for("profile"))

        ext = os.path.splitext(f.filename)[1].lower().lstrip(".")
        if ext not in {"png", "jpg", "jpeg", "webp", "gif"}:
            flash("Please upload an image file (PNG, JPG, WebP, or GIF)")
            return redirect(url_for("profile"))

        try:
            img = Image.open(f.stream).convert("RGB")
                                               
            img.thumbnail((256, 256), Image.LANCZOS)
        except Exception:
            flash("Could not process image")
            return redirect(url_for("profile"))

        blob_name = f"avatars/{user['_id']}.png"
        if blob.is_configured():
            from io import BytesIO
            buf = BytesIO()
            img.save(buf, format="PNG")
            blob.upload_bytes(
                container=app.config["UPLOADS_CONTAINER"],
                blob_name=blob_name,
                data=buf.getvalue(),
                content_type="image/png",
            )
        else:
            avatar_dir = os.path.join(app.config["UPLOAD_FOLDER"], "avatars")
            os.makedirs(avatar_dir, exist_ok=True)
            local_path = os.path.join(app.config["UPLOAD_FOLDER"], blob_name)
            img.save(local_path, format="PNG")

        users.update_user(user["_id"], {"profile_picture": blob_name})
        flash("Profile picture updated")
        return redirect(url_for("profile"))

    @app.route("/catalog", methods=["GET"], endpoint="catalog")
    def catalog():
        if analyzer is None or users is None:
            flash("Database is not configured.")
            return render_template("catalog.html", items=[]), 503
        bu_code = None
        if session.get("user_id"):
            u = users.get_user_by_id(session.get("user_id"))
            bu_code = u.get("bu_code") if u else None

                                                       
        coll = analyzer.bricks_collection
        docs = list(
            coll.find(
                {"$or": [{"dataset": "catalog"}, {"dataset": {"$exists": False}}]},
                {
                    "_id": 1,
                    "brick_id": 1,
                    "image_path": 1,
                    "metadata": 1,
                    "factory_code": 1,
                    "factory_region": 1,
                },
            ).limit(24)
        )

        items = []
        from lib.bu_pricing import get_bu_pricing

        for d in docs:
            md = d.get("metadata") or {}
            item = {
                "id": str(d.get("_id")),
                "image_path": d.get("image_path"),
                "name": md.get("display_name") or md.get("brick_name") or md.get("item_number") or md.get("item_code") or str(d.get("_id")),
                "colour": md.get("colour") or md.get("brick_colour"),
                "type": md.get("type") or md.get("brick_type"),
                "brand": md.get("brand"),
                "bu_pricing": None,
            }
            if bu_code:
                item["bu_pricing"] = get_bu_pricing(bu_code, {**md, **d})
            items.append(item)

        return render_template("catalog.html", items=items)

    @app.route("/results/<brick_id>", methods=["GET"], endpoint="results")
    def results(brick_id: str):
        if analyzer is None:
            flash("Database is not configured.")
            return render_template("search.html"), 503
        doc = analyzer.bricks_collection.find_one({"$or": [{"brick_id": brick_id}, {"_id": brick_id}]})
        if not doc:
            return ("Not found", 404)

        metadata = doc.get("metadata") or {}

        image_path = doc.get("image_path")
        image_url = url_for("uploads", relpath=image_path) if image_path else None

        matches = []
        try:
            if image_path:
                img = load_image(
                    image_path,
                    blob=blob,
                    uploads_container=app.config["UPLOADS_CONTAINER"],
                    upload_folder=app.config["UPLOAD_FOLDER"],
                )
                                                          
                candidate_matches = analyzer.search_similar_bricks(img, top_k=6, dataset="catalog")
                self_id = str(doc.get("brick_id") or doc.get("_id"))
                matches = [m for m in candidate_matches if str(m._id) != self_id][:5]
        except Exception as e:
            logger.warning(f"Could not compute similar bricks for {brick_id}: {e}")

                                                 
        from lib.bu_locations_static import get_bu_location, list_bu_locations
        from lib.factory_service import factory_address_string, get_factory_by_code
        from lib.google_maps_distance import GoogleMapsError, embed_directions_iframe_url, get_driving_distance_km

        bu_locations = list_bu_locations()
        selected_bu_id = (
            request.args.get("bu")
            or session.get("selected_bu")
            or getattr(g, "user_bu_code", None)
            or ""
        ).strip() or None
        if selected_bu_id:
            session["selected_bu"] = selected_bu_id
        selected_bu = get_bu_location(selected_bu_id) if selected_bu_id else None

        google_maps_key = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

                                                                                       
        proc = metadata.get("procurement") or {}
        factory_code = (
            doc.get("factory_code")
            or metadata.get("factory_code")
            or proc.get("home_factory_code")
        )
        factory_doc = get_factory_by_code(str(factory_code)) if factory_code else None
        factory_origin = (
            factory_address_string(factory_doc)
            or doc.get("factory_address")
            or metadata.get("factory_address")
        )

                                                                      
        if not factory_origin:
            factory_origin = metadata.get("factory") or doc.get("factory")

        distance_info = None
        distance_error = None
        if selected_bu and factory_origin and google_maps_key:
            try:
                km = get_driving_distance_km(
                    api_key=google_maps_key,
                    origin=str(factory_origin),
                    destination=str(selected_bu.get("address")),
                    timeout_s=6.0,
                )
                if km is not None:
                    co2_factor = float(os.getenv("TRUCK_CO2_KG_PER_KM", "0.9"))
                    co2_kg = km * co2_factor
                    distance_info = {
                        "origin": str(factory_origin),
                        "destination": str(selected_bu.get("address")),
                        "distance_km": km,
                        "co2_kg": co2_kg,
                        "co2_factor_kg_per_km": co2_factor,
                        "map_iframe_url": embed_directions_iframe_url(
                            api_key=google_maps_key,
                            origin=str(factory_origin),
                            destination=str(selected_bu.get("address")),
                        ),
                    }
            except (GoogleMapsError, Exception) as e:
                logger.warning(f"Could not compute distance/CO2 for {brick_id}: {e}")
                distance_error = str(e)

        return render_template(
            "results.html",
            brick_id=str(doc.get("brick_id") or doc.get("_id")),
            brick=doc,
            metadata=metadata,
            image_url=image_url,
            matches=matches,
            bu_locations=bu_locations,
            selected_bu_id=selected_bu_id,
            selected_bu=selected_bu,
            distance_info=distance_info,
            distance_error=distance_error,
            google_maps_enabled=bool(google_maps_key),
            factory_origin=factory_origin,
            factory_code=factory_code,
        )

    @app.route("/search", methods=["GET", "POST"], endpoint="search")
    def search():
        if analyzer is None:
            flash("Database is not configured.")
            return render_template("search.html"), 503
        if request.method == "GET":
            return render_template("search.html")

        f = request.files.get("image")
        if not f or not f.filename:
            flash("No file uploaded")
            return render_template("search.html"), 400

        ext = os.path.splitext(f.filename)[1].lower().lstrip(".")
        if ext not in app.config["ALLOWED_EXTENSIONS"]:
            flash("Unsupported file type")
            return render_template("search.html"), 400

        top_k = int(request.form.get("top_k") or 10)
        img = Image.open(f.stream).convert("RGB")

        matches = analyzer.search_similar_bricks(img, top_k=top_k, dataset="catalog")
        if not matches:
            flash("No catalog bricks found to search against.")
        return render_template("search_results.html", matches=matches)

    @app.route("/ingest", methods=["GET", "POST"], endpoint="ingest")
    @admin_required
    def ingest():
        if analyzer is None:
            flash("Database is not configured.")
            return render_template("ingest.html"), 503

        if request.method == "GET":
            return render_template("ingest.html")

        f = request.files.get("image")
        if not f or not f.filename:
            flash("No file uploaded")
            return render_template("ingest.html"), 400

        ext = os.path.splitext(f.filename)[1].lower().lstrip(".")
        if ext not in app.config["ALLOWED_EXTENSIONS"]:
            flash("Unsupported file type")
            return render_template("ingest.html"), 400

        img = Image.open(f.stream).convert("RGB")

        metadata = {
            "brick_name": (request.form.get("brick_name") or "").strip() or None,
            "item_number": (request.form.get("item_number") or "").strip() or None,
            "brick_colour": (request.form.get("brick_colour") or "").strip() or None,
            "brick_type": (request.form.get("brick_type") or "").strip() or None,
            "brand": (request.form.get("brand") or "").strip() or None,
            "material": (request.form.get("material") or "").strip() or None,
            "factory_code": (request.form.get("factory_code") or "").strip() or None,
            "factory_region": (request.form.get("factory_region") or "").strip() or None,
        }

                                         
        relpath = None
        filename = os.path.basename(f.filename)
        stem = os.path.splitext(filename)[0].strip().replace(" ", "_") or "brick"
        blob_name = f"{secrets.token_hex(8)}_{stem}.png"
        if blob.is_configured():
            from io import BytesIO

            buf = BytesIO()
            img.save(buf, format="PNG")
            relpath = blob.upload_bytes(
                container=app.config["UPLOADS_CONTAINER"],
                blob_name=blob_name,
                data=buf.getvalue(),
                content_type="image/png",
            )
        else:
            relpath = blob_name
            local_path = os.path.join(app.config["UPLOAD_FOLDER"], relpath)
            img.save(local_path, format="PNG")

                                                                           
        brick_id = analyzer.process_and_store_brick(img, relpath=relpath, metadata=metadata, dataset="catalog")

                                                           
        from lib.brick_pricing import upsert_brick_pricing

        band_asps = {}
        for k in ("asp_T1", "asp_T2", "asp_T3", "asp_T4"):
            v = (request.form.get(k) or "").strip()
            if v:
                try:
                    band_asps[k] = v
                except Exception:
                    band_asps[k] = v

        if band_asps:
            upsert_brick_pricing(
                brick_id=brick_id,
                band_asps=band_asps,
                factory_code=metadata.get("factory_code"),
                factory_region=metadata.get("factory_region"),
            )

        flash(f"Ingested brick {brick_id}")
        return redirect(url_for("results", brick_id=brick_id))

    @app.route("/compare", methods=["GET"], endpoint="compare")
    def compare():
        if analyzer is None:
            flash("Database is not configured.")
            return render_template("compare.html", left_id=None, right_id=None, comparison=None, pool=[]), 503
        left_id = (request.args.get("left") or "").strip() or None
        right_id = (request.args.get("right") or "").strip() or None
        bu_code = (request.args.get("bu") or "").strip() or (getattr(g, "user_bu_code", None) or None)
        comparison = None
        left_card = None
        right_card = None
        bu_locations = []

        try:
            from lib.bu_locations_static import list_bu_locations

            bu_locations = list_bu_locations()
        except Exception:
            bu_locations = []

                                       
        pool = []
        calc_pool = []
        try:
            cursor = analyzer.bricks_collection.find(
                {"dataset": "catalog"},
                {
                    "_id": 1,
                    "brick_id": 1,
                    "image_path": 1,
                    "metadata.display_name": 1,
                    "metadata.brick_name": 1,
                    "metadata.item_code": 1,
                    "metadata.item_number": 1,
                    "metadata.pack_size": 1,
                    "metadata.brand": 1,
                    "metadata.factory": 1,
                    "metadata.colour": 1,
                    "metadata.brick_colour": 1,
                },
            )
            for d in cursor:
                bid = str(d.get("brick_id") or d.get("_id") or "")
                if not bid:
                    continue
                md = d.get("metadata") or {}
                name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or bid
                brand = md.get("brand")
                colour = md.get("colour") or md.get("brick_colour")
                bits = [str(name)]
                if brand:
                    bits.append(str(brand))
                if colour:
                    bits.append(str(colour))
                label = " — ".join(bits)
                pool.append({"id": bid, "label": label})

                sku = (md.get("item_number") or md.get("item_code") or "").strip()
                if sku:
                    calc_pool.append({"sku": sku, "name": name})
            pool.sort(key=lambda x: (x.get("label") or "").lower())
                                                        
            seen = set()
            deduped = []
            for b in sorted(calc_pool, key=lambda x: (x.get("sku") or "").lower()):
                s = (b.get("sku") or "").strip()
                if not s or s in seen:
                    continue
                seen.add(s)
                deduped.append(b)
            calc_pool = deduped
        except Exception:
            pool = []
            calc_pool = []

        if left_id and right_id:
            if left_id == right_id:
                flash("Pick two different bricks to compare.")
                return render_template(
                    "compare.html",
                    left_id=left_id,
                    right_id=right_id,
                    bu_code=bu_code,
                    comparison=None,
                    left_card=None,
                    right_card=None,
                    bu_locations=bu_locations,
                    pool=pool,
                    calc_pool=calc_pool,
                )
            left = analyzer.bricks_collection.find_one({"$or": [{"brick_id": left_id}, {"_id": left_id}]})
            right = analyzer.bricks_collection.find_one({"$or": [{"brick_id": right_id}, {"_id": right_id}]})
            if left and right:
                                                          
                                                                      
                try:
                    left_img = load_image(
                        left.get("image_path"),
                        blob=blob,
                        uploads_container=app.config["UPLOADS_CONTAINER"],
                        upload_folder=app.config["UPLOAD_FOLDER"],
                    )
                    right_img = load_image(
                        right.get("image_path"),
                        blob=blob,
                        uploads_container=app.config["UPLOADS_CONTAINER"],
                        upload_folder=app.config["UPLOAD_FOLDER"],
                    )

                    def _relpath_to_url(p: str | None) -> str | None:
                        if not p:
                            return None
                        s = str(p).replace("\\", "/")
                        if s.startswith("uploads/"):
                            s = s[len("uploads/") :]
                                                                                        
                        if ":/" in s or s.startswith("/"):
                            s = os.path.basename(s)
                        return url_for("uploads", relpath=s)

                    def _to_card(doc: dict) -> dict:
                        md = doc.get("metadata") or {}
                        name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or "(unknown)"
                        return {
                            "name": name,
                            "brand": md.get("brand"),
                            "colour": md.get("colour") or md.get("brick_colour"),
                            "item_code": md.get("item_code"),
                            "factory": md.get("factory") or md.get("factory_code") or doc.get("factory_code"),
                            "image_url": _relpath_to_url(doc.get("image_path")),
                        }

                    left_card = _to_card(left)
                    right_card = _to_card(right)

                    colour = float(analyzer.color_similarity_ab(left_img, right_img))
                    texture = float(analyzer.texture_similarity_relief(left_img, right_img))
                    defects = float(analyzer.discriminative_similarity(left_img, right_img))
                                                                      
                    left_mat = (left.get("metadata") or {}).get("material")
                    right_mat = (right.get("metadata") or {}).get("material")
                    material_sim = 1.0
                    if left_mat and right_mat and str(left_mat).strip().lower() != str(right_mat).strip().lower():
                        material_sim = 0.85

                    overall = (0.65 * colour + 0.25 * texture + 0.10 * defects) * material_sim

                                     
                    left_colour = compute_colour_stats(left_img)
                    right_colour = compute_colour_stats(right_img)
                    colour_delta = delta_e76(left_colour.avg_lab, right_colour.avg_lab)

                    left_tex = compute_texture_stats(left_img)
                    right_tex = compute_texture_stats(right_img)

                    left_overlay = png_bytes_to_data_url(
                        make_difference_heatmap_overlay_png(left_img, right_img, base="left")
                    )
                    right_overlay = png_bytes_to_data_url(
                        make_difference_heatmap_overlay_png(left_img, right_img, base="right")
                    )

                                                    
                    yolo = None
                    left_defects_overlay = None
                    right_defects_overlay = None
                    left_defects = []
                    right_defects = []
                    yolo_status = None
                    try:
                        from lib.yolo_defect_detector import get_defect_detector

                        yolo = get_defect_detector()
                        try:
                            yolo_status = yolo.get_status() if yolo else None
                        except Exception:
                            yolo_status = None
                        if yolo and yolo.is_available():
                            left_defects = yolo.detect_defects(left_img)
                            right_defects = yolo.detect_defects(right_img)
                            left_defects_overlay = png_bytes_to_data_url(
                                render_defects_overlay_png(left_img, left_defects)
                            )
                            right_defects_overlay = png_bytes_to_data_url(
                                render_defects_overlay_png(right_img, right_defects)
                            )
                    except Exception:
                        pass

                                                        
                    from lib.bu_pricing import get_bu_pricing
                    from lib.brick_pricing import get_asp_for_band

                    left_bid = str(left.get("brick_id") or left.get("_id") or "").strip() or None
                    right_bid = str(right.get("brick_id") or right.get("_id") or "").strip() or None
                    left_prices = {
                        "T1": get_asp_for_band(left_bid, "T1") if left_bid else None,
                        "T2": get_asp_for_band(left_bid, "T2") if left_bid else None,
                        "T3": get_asp_for_band(left_bid, "T3") if left_bid else None,
                        "T4": get_asp_for_band(left_bid, "T4") if left_bid else None,
                    }
                    right_prices = {
                        "T1": get_asp_for_band(right_bid, "T1") if right_bid else None,
                        "T2": get_asp_for_band(right_bid, "T2") if right_bid else None,
                        "T3": get_asp_for_band(right_bid, "T3") if right_bid else None,
                        "T4": get_asp_for_band(right_bid, "T4") if right_bid else None,
                    }

                    left_bu = get_bu_pricing(bu_code, left) if bu_code else None
                    right_bu = get_bu_pricing(bu_code, right) if bu_code else None

                    pricing = {
                        "bu_code": bu_code,
                        "left": left_bu,
                        "right": right_bu,
                        "left_summary": build_pricing_summary(bu_code=bu_code, pricing=left_bu),
                        "right_summary": build_pricing_summary(bu_code=bu_code, pricing=right_bu),
                        "left_bands": left_prices,
                        "right_bands": right_prices,
                    }

                                                                           
                    llm = get_llm_service()
                    summary = None
                    try:
                        md_l = left.get("metadata") or {}
                        md_r = right.get("metadata") or {}
                        prompt = (
                            "Write a concise but insightful comparison of two bricks for a construction buyer. "
                            "Use ONLY the facts provided. Do not invent specs or certifications. "
                            "Mention colour/texture differences, defects/pattern similarity, and pricing context if present.\n\n"
                            f"LEFT: name={md_l.get('display_name') or md_l.get('brick_name') or md_l.get('item_code')}, "
                            f"brand={md_l.get('brand')}, colour={md_l.get('colour') or md_l.get('brick_colour')}, item_code={md_l.get('item_code')}\n"
                            f"RIGHT: name={md_r.get('display_name') or md_r.get('brick_name') or md_r.get('item_code')}, "
                            f"brand={md_r.get('brand')}, colour={md_r.get('colour') or md_r.get('brick_colour')}, item_code={md_r.get('item_code')}\n\n"
                            f"Similarity: overall={overall:.3f}, colour_similarity={colour:.3f}, texture_similarity={texture:.3f}, pattern_similarity={defects:.3f}\n"
                            f"Colour deltaE≈{colour_delta:.1f} ({describe_colour_delta(colour_delta)})\n"
                            f"Texture stats left: lap_var={left_tex.laplacian_var:.1f}, edge_density={left_tex.edge_density:.3f}\n"
                            f"Texture stats right: lap_var={right_tex.laplacian_var:.1f}, edge_density={right_tex.edge_density:.3f}\n"
                            f"BU pricing context: {bu_code or 'none'}\n"
                            f"Left pricing: {pricing.get('left_summary')}\n"
                            f"Right pricing: {pricing.get('right_summary')}\n"
                        )
                        resp = llm.chat(
                            [
                                {"role": "system", "content": "You are Brick AI. You only discuss bricks using provided catalog data."},
                                {"role": "user", "content": prompt},
                            ],
                            max_tokens=240,
                            temperature=0.6,
                        )
                        summary = (resp or "").strip() or None
                    except Exception:
                        summary = None

                    if not summary:
                        summary = (
                            f"Overall match: {overall*100:.1f}%. Colour is {describe_colour_delta(colour_delta).lower()} (ΔE≈{colour_delta:.1f}). "
                            f"Texture similarity is {texture*100:.1f}% with edge density {left_tex.edge_density:.3f} vs {right_tex.edge_density:.3f}. "
                            f"Pattern/defects similarity is {defects*100:.1f}%. "
                            "If you set a BU code, I can show region-aware pricing for both bricks."
                        )

                    comparison = {
                        "overall": overall,
                        "colour": colour,
                        "texture": texture,
                        "defects": defects,
                        "colour_delta_e": colour_delta,
                        "colour_delta_desc": describe_colour_delta(colour_delta),
                        "left_avg_colour_hex": rgb_hex(left_colour.avg_rgb),
                        "right_avg_colour_hex": rgb_hex(right_colour.avg_rgb),
                        "left_texture": {
                            "laplacian_var": left_tex.laplacian_var,
                            "edge_density": left_tex.edge_density,
                        },
                        "right_texture": {
                            "laplacian_var": right_tex.laplacian_var,
                            "edge_density": right_tex.edge_density,
                        },
                        "heatmap": {
                            "left_overlay": left_overlay,
                            "right_overlay": right_overlay,
                        },
                        "defects_overlay": {
                            "available": bool(yolo and yolo.is_available()),
                            "status": yolo_status,
                            "left_overlay": left_defects_overlay,
                            "right_overlay": right_defects_overlay,
                            "left_count": len(left_defects) if left_defects else 0,
                            "right_count": len(right_defects) if right_defects else 0,
                        },
                        "pricing": pricing,
                        "ai_summary": summary,
                    }
                except Exception as e:
                    flash(f"Compare failed: {e}")

        return render_template(
            "compare.html",
            left_id=left_id,
            right_id=right_id,
            bu_code=bu_code,
            comparison=comparison,
            left_card=left_card,
            right_card=right_card,
            bu_locations=bu_locations,
            pool=pool,
            calc_pool=calc_pool,
        )

    def _strip_tw_prefix(region_name: str) -> str:
        v = (region_name or "").strip()
        if not v:
            return v
        return re.sub(r"^\s*taylor\s+wimpey\s+", "", v, flags=re.IGNORECASE).strip()

    def _region_name_to_bu_code(tw_region_name: str) -> str | None:
        v = _strip_tw_prefix(tw_region_name)
        if not v:
            return None
        try:
            from lib.bu_locations_static import list_bu_locations

            for bu in list_bu_locations() or []:
                name = (bu.get("name") or "").strip()
                if name and name.lower() == v.lower():
                    return (bu.get("id") or "").strip() or None
        except Exception:
            pass
                                                                       
        return v.upper().replace("-", "_").replace(" ", "_")

    _FACTORY_NAME_TO_CODE = {
        "ATLAS": "ATL",
        "CATTYBROOK": "CAT",
        "CHESTERTON": "CHE",
        "DORKET HEAD": "DOR",
        "ELLISTOWN": "ELL",
        "ECLIPSE": "LE3",
        "LODGE LANE": "LOD",
        "PARKHOUSE": "PAR",
        "THROCKLEY": "THR",
    }

    @app.route("/api/calc/bricks", methods=["POST"])
    def api_calc_bricks():
        if analyzer is None:
            return jsonify({"error": "Database is not configured."}), 503

        payload = request.get_json(silent=True) or {}
        sku = (payload.get("brick_sku") or "").strip()
        if not sku:
            return jsonify({"error": "brick_sku is required"}), 400

                                    
        sku_candidates = list(dict.fromkeys([sku, sku.upper(), sku.lower()]))
        brick = analyzer.bricks_collection.find_one(
            {
                "dataset": "catalog",
                "$or": [
                    {"metadata.item_number": {"$in": sku_candidates}},
                    {"metadata.item_code": {"$in": sku_candidates}},
                ],
            }
        )
        if not brick:
            return jsonify({"error": f"No catalog brick found for SKU '{sku}'."}), 404

        brick_id = str(brick.get("brick_id") or brick.get("_id") or "").strip()
        if not brick_id:
            return jsonify({"error": "Brick document is missing an id."}), 500

                                                                               
        enriched = deepcopy(brick)
        md = enriched.setdefault("metadata", {})
        pricing_md = md.setdefault("pricing", {})

                                                   
        try:
            from bson.decimal128 import Decimal128
            from lib.brick_pricing import get_brick_pricing

            pricing_doc = get_brick_pricing(brick_id)
            band_asps = (pricing_doc or {}).get("band_asps") or {}
            tier_prices: dict[str, float] = {}
            for band in ("T1", "T2", "T3", "T4"):
                v = band_asps.get(band)
                if isinstance(v, Decimal128):
                    try:
                        tier_prices[band] = float(v.to_decimal())
                    except Exception:
                        continue
                else:
                    try:
                        tier_prices[band] = float(v)
                    except Exception:
                        continue
            if tier_prices:
                pricing_md.setdefault("tier_prices_gbp_per_th", tier_prices)
        except Exception:
            pass

                                                                                      
        try:
            pricing_in = payload.get("pricing") or {}
            if (pricing_in.get("price_mode") == "tiered_region") and pricing_in.get("tw_region_name") and pricing_in.get(
                "selected_factory"
            ):
                tw_region_name = str(pricing_in.get("tw_region_name") or "").strip()
                factory_name = str(pricing_in.get("selected_factory") or "").strip()
                factory_code = _FACTORY_NAME_TO_CODE.get(factory_name.upper())
                if not factory_code:
                    return jsonify({"error": "Unknown factory selected."}), 400

                bu_code = _region_name_to_bu_code(tw_region_name)
                if not bu_code:
                    return jsonify({"error": "Invalid tw_region_name."}), 400

                from lib.mongo import get_db

                db = get_db()
                rule = db.bu_factory_pricing.find_one(
                    {
                        "bu_code": bu_code,
                        "factory_code": factory_code,
                        "$or": [{"effective_to": None}, {"effective_to": {"$exists": False}}],
                    }
                )
                if not rule or not rule.get("price_band"):
                    return jsonify({"error": "No tier rule found for that region + factory."}), 400

                tier = str(rule.get("price_band") or "").strip()
                pricing_md["tw_regions"] = [
                    {
                        "tw_region_name": tw_region_name,
                        "factory_tiers": {factory_name: tier},
                    }
                ]
        except Exception:
            pass

        try:
            from lib.brick_calc import ValidationError, calculate_bricks_and_cost

            result = calculate_bricks_and_cost(payload, enriched)
            return jsonify(result)
        except ValidationError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.exception("/api/calc/bricks failed")
            return jsonify({"error": str(e) or "Calculation failed."}), 500

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    debug = str(os.getenv("FLASK_DEBUG", "1")).strip().lower() in ("1", "true", "yes", "on")

                                                                               
                                                                              
                                                  
                                                            
    use_reloader = str(os.getenv("FLASK_USE_RELOADER", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    app.run(host="0.0.0.0", port=port, debug=debug, use_reloader=use_reloader)
