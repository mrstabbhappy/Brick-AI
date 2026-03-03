from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from lib.image_store import load_image
from lib.llm_service import LLMService
from lib.bu_pricing import get_bu_pricing
from lib.brick_pricing import get_brick_pricing
from lib.mongo import get_db


class BrickAssistant:
    def __init__(
        self,
        llm: LLMService,
        *,
        analyzer: Any = None,
        blob: Any = None,
        uploads_container: str = "uploads",
        upload_folder: str = "uploads",
    ):
        self.llm = llm
        self.analyzer = analyzer
        self.blob = blob
        self.uploads_container = uploads_container
        self.upload_folder = upload_folder

    def _llm_chat(self, messages: List[Dict[str, str]], **kwargs) -> Optional[str]:
        """Best-effort LLM call.

        We keep guardrails in prompts, but we also expect the LLM may be
        unavailable (no key) or blocked by Azure firewall/VNet.
        """
        try:
            text = self.llm.chat(messages, **kwargs)
            if not text:
                return None
            lowered = text.lower()
            if "blocked by azure openai network security" in lowered:
                return None
            if "ai is not configured" in lowered:
                return None
            if "temporarily unavailable" in lowered:
                return None
            return text
        except Exception:
            return None

    def _system_prompt(self) -> str:
        return (
            "You are Brick AI, a helpful assistant for a brick catalog application.\n"
            "STYLE: Sound natural and friendly. Be concise.\n"
            "GUARDRAILS (must follow):\n"
            "- Only answer about bricks in the user's catalog/database, including brick metadata and pricing bands.\n"
            "- Do NOT claim product-specific facts that are not in the catalog context.\n"
            "- You MAY give general construction guidance (best-effort) when asked, but label it as general guidance and keep it high-level.\n"
            "- If the catalog context is insufficient, ask a short clarifying question or ask the user to use image search / provide the item code.\n"
            "- For pricing: only use the provided pricing data; if missing, say it's not stored yet.\n"
        )

    def chat(
        self,
        message: str,
        conversation_history: List[Dict[str, str]],
        bu_code: Optional[str] = None,
        choice_ids: Optional[List[str]] = None,
        pending_intent: Optional[str] = None,
        selected_brick_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Brick-only assistant.

        Guardrails:
        - Only answer about bricks in the catalog and pricing data in the DB.
        - Do not use general/web knowledge. If not in the catalog, say so.
        """

        user_text = (message or "").strip()
        msg_lower = user_text.lower()

        if not user_text:
            return {"text": "Ask me about a brick in the catalog, or paste an item code.", "intent": "help"}

                                                                     
        lifecycle = self._classify_lifecycle_intent(msg_lower)
        if selected_brick_id and lifecycle == "info":
            b = self._get_brick_by_id(selected_brick_id)
            if b:
                return self._more_about_brick(b, bu_code=bu_code)
        if selected_brick_id and lifecycle == "similar":
            return self._similar_options_for_brick(selected_brick_id)
        if selected_brick_id and lifecycle == "compare":
            return {
                "text": "Sure — what should I compare it with? Type the other brick name.",
                "intent": "compare_prompt",
                "_pending_action": "compare",
            }
        if selected_brick_id and lifecycle in ("specs", "advice"):
            b = self._get_brick_by_id(selected_brick_id)
            if b:
                if lifecycle == "specs":
                    return self._tech_specs_for_brick(b)
                return self._advice_for_brick(b)

                                                                                       
        chosen = self._resolve_choice_from_choice_ids(user_text, choice_ids) or self._resolve_choice_from_history(
            user_text, conversation_history
        )
        if chosen is not None:
            chosen_id = str(chosen.get("brick_id") or chosen.get("_id") or "").strip() or None

            if pending_intent == "pricing":
                resp = self._pricing_for_brick(chosen, bu_code=bu_code)
                if chosen_id:
                    resp["_selected_brick_id"] = chosen_id
                return resp

            if pending_intent == "compare" and selected_brick_id:
                left = self._get_brick_by_id(selected_brick_id)
                if left:
                    resp = self._compare_two_bricks(left, chosen)
                    if chosen_id:
                        resp["_selected_brick_id"] = chosen_id
                    return resp

            summary = self._format_brick_summary(chosen, bu_code=bu_code)
            messages = [
                {"role": "system", "content": self._system_prompt()},
                {"role": "system", "content": "CATALOG CONTEXT (authoritative):\n" + summary},
                {"role": "user", "content": "Confirm the selected brick in a friendly tone and ask what they'd like next (price, compare, or similar search)."},
            ]
            text = self._llm_chat(messages, max_tokens=180, temperature=0.5)
            resp: Dict[str, Any] = {"text": text or summary, "intent": "brick_info"}
            if chosen_id:
                resp["_selected_brick_id"] = chosen_id
            return resp

                        
        if any(w in msg_lower for w in ("price", "cost", "asp", "pricing")):
            return self._handle_pricing_question(user_text, bu_code=bu_code)

                                                                                          
        if pending_intent == "compare" and selected_brick_id:
            matches = self._find_bricks(user_text, limit=5)
            if matches and len(matches) == 1:
                left = self._get_brick_by_id(selected_brick_id)
                if left:
                    resp = self._compare_two_bricks(left, matches[0])
                    rid = str(matches[0].get("brick_id") or matches[0].get("_id") or "").strip()
                    if rid:
                        resp["_selected_brick_id"] = rid
                    return resp

            if matches and len(matches) > 1:
                disambig, ids = self._format_disambiguation(matches, prefix="Which one should I compare it to?")
                cards = self._build_cards(matches)
                resp = {
                    "text": "I found a couple of matches — click the one you meant.",
                    "intent": "brick_disambiguation",
                    "cards": cards,
                    "_choice_ids": ids,
                    "_choice_mode": "compare",
                }
                return resp

            return {
                "text": "I couldn’t find that second brick in the catalog. Try a shorter name (or use Search by image).",
                "intent": "compare_prompt",
                "_pending_action": "compare",
            }

                                                                                                                       
        matches = self._find_bricks(user_text, limit=5)
        if matches:
            if len(matches) == 1:
                                                                              
                summary = self._format_brick_summary(matches[0], bu_code=bu_code)
                messages = [
                    {"role": "system", "content": self._system_prompt()},
                    {
                        "role": "system",
                        "content": "CATALOG CONTEXT (authoritative):\n" + summary,
                    },
                    {"role": "user", "content": user_text},
                ]
                text = self._llm_chat(messages, max_tokens=220, temperature=0.4)
                resp: Dict[str, Any] = {"text": text or summary, "intent": "brick_info"}
                bid = str(matches[0].get("brick_id") or matches[0].get("_id") or "").strip()
                if bid:
                    resp["_selected_brick_id"] = bid
                return resp

            disambig, ids = self._format_disambiguation(matches)
            cards = self._build_cards(matches)
            messages = [
                {"role": "system", "content": self._system_prompt()},
                {
                    "role": "system",
                    "content": "CATALOG MATCHES (authoritative):\n" + disambig,
                },
                {
                    "role": "user",
                    "content": (
                        "The user asked: "
                        + user_text
                        + "\nAsk a friendly clarifying question. The UI will show clickable brick cards; tell them to click the right one."
                    ),
                },
            ]
            text = self._llm_chat(messages, max_tokens=220, temperature=0.5)
            resp: Dict[str, Any] = {"text": text or disambig, "intent": "brick_disambiguation", "cards": cards}
            if ids:
                                                                                        
                resp["_choice_ids"] = ids
                resp["_choice_mode"] = "info"
            return resp

                                                                                                    
        if not self._looks_brick_related(msg_lower):
            return {
                "text": "I can help with brick catalog questions (identifying bricks, comparing, and pricing from our database). Try a brick name, item code, or upload an image in Search.",
                "intent": "out_of_scope",
            }

                                                                           
        return {
            "text": "I can’t find that brick in the catalog from the text provided. If you have an item code, brick_id, or an image, I can match it and show details.",
            "intent": "not_found",
        }

    def _handle_pricing_question(self, message: str, bu_code: Optional[str]) -> Dict[str, Any]:
        matches = self._find_bricks(message, limit=5)
        if not matches:
            if not bu_code:
                return {
                    "text": "I can answer pricing from our catalog data, but I need a brick identifier (name/item code) and your BU code. Set your BU in Profile, then ask e.g. ‘price for <item code>’.",
                    "intent": "pricing",
                }
            return {
                "text": "I can’t find that brick in the catalog to price it. Try the item code / brick_id (or upload an image in Search).",
                "intent": "pricing",
            }

        if len(matches) > 1:
            disambig, ids = self._format_disambiguation(matches, prefix="Which brick do you mean for pricing?")
            cards = self._build_cards(matches)
            messages = [
                {"role": "system", "content": self._system_prompt()},
                {"role": "system", "content": "CATALOG MATCHES (authoritative):\n" + disambig},
                {
                    "role": "user",
                    "content": (
                        "Ask a friendly clarifying question for pricing. "
                        "Do not invent prices. The UI will show clickable brick cards; tell them to click the right one."
                    ),
                },
            ]
            text = self._llm_chat(messages, max_tokens=200, temperature=0.5)
            resp: Dict[str, Any] = {"text": text or disambig, "intent": "pricing", "cards": cards}
            if ids:
                resp["_choice_ids"] = ids
                resp["_choice_mode"] = "pricing"
            return resp

        return self._pricing_for_brick(matches[0], bu_code=bu_code)

    def _classify_lifecycle_intent(self, msg_lower: str) -> Optional[str]:
                                                                                     
        if any(k in msg_lower for k in ("tell me more", "more about", "more details", "details", "about that", "about this", "that brick", "this brick")):
            return "info"
        if any(k in msg_lower for k in ("similar", "alternatives", "other options", "like this", "matches")):
            return "similar"
        if "compare" in msg_lower:
            return "compare"
        if any(k in msg_lower for k in ("spec", "specs", "technical", "dimensions", "material", "weight")):
            return "specs"
        if any(k in msg_lower for k in ("what projects", "where to use", "suitable", "recommend", "use it for")):
            return "advice"
        return None

    def _more_about_brick(self, brick: dict, bu_code: Optional[str]) -> Dict[str, Any]:
        md = brick.get("metadata") or {}
        name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or "This brick"

                                                                             
        lines = [f"Brick: {name}"]
        for label, key in (
            ("Item code", "item_code"),
            ("Brand", "brand"),
            ("Colour", "colour"),
            ("Factory", "factory"),
            ("Factory code", "factory_code"),
            ("Finish", "finish"),
            ("Texture", "texture"),
            ("Material", "material"),
            ("Format", "format"),
            ("Size", "size"),
            ("Dimensions", "dimensions"),
        ):
            v = md.get(key)
            if v:
                lines.append(f"{label}: {v}")

                                       
        if bu_code:
            try:
                bu = get_bu_pricing(bu_code, brick)
                if bu.get("bu_band") and bu.get("bu_asp") is not None:
                    extra = "" if bu.get("in_region") else " (out-of-region/default band)"
                    lines.append(f"BU {bu_code} price: band {bu.get('bu_band')}{extra}, ASP {bu.get('bu_asp')}")
            except Exception:
                pass

        grounded = "\n".join(lines)
        messages = [
            {"role": "system", "content": self._system_prompt()},
            {"role": "system", "content": "CATALOG CONTEXT (authoritative):\n" + grounded},
            {
                "role": "user",
                "content": "Tell the user more about this brick using only the catalog context. If something isn't in the context, say so and suggest what they can ask next (similar options, price, compare, specs, project advice).",
            },
        ]
        text = self._llm_chat(messages, max_tokens=260, temperature=0.6)
        return {"text": text or grounded, "intent": "brick_info"}

    def _get_brick_by_id(self, brick_id: str) -> Optional[dict]:
        if not brick_id:
            return None
        try:
            if self.analyzer is not None and getattr(self.analyzer, "bricks_collection", None) is not None:
                return self.analyzer.bricks_collection.find_one({"$or": [{"brick_id": brick_id}, {"_id": brick_id}]})
        except Exception:
            pass
        try:
            db = get_db()
            return db.bricks.find_one({"$or": [{"brick_id": brick_id}, {"_id": brick_id}]})
        except Exception:
            return None

    def _similar_options_for_brick(self, brick_id: str) -> Dict[str, Any]:
        if self.analyzer is None:
            return {
                "text": "Similarity search isn’t available right now (analyzer not initialized).",
                "intent": "similar",
            }

        brick = self._get_brick_by_id(brick_id)
        if not brick:
            return {"text": "I can’t find the selected brick in the catalog anymore.", "intent": "similar"}

        image_ref = brick.get("image_path")
        if not image_ref:
            return {"text": "That brick doesn’t have an image in the catalog, so I can’t run similarity search.", "intent": "similar"}

        try:
            img = load_image(
                image_ref,
                blob=self.blob,
                uploads_container=self.uploads_container,
                upload_folder=self.upload_folder,
            )
            results = self.analyzer.search_similar_bricks(img, top_k=8, dataset="catalog")
        except Exception as e:
            return {"text": f"I couldn’t run similarity search right now: {e}", "intent": "similar"}

                                    
        filtered = [r for r in results if str(getattr(r, "_id", "")) != str(brick_id)]
        cards, ids = self._build_cards_from_matches(filtered)

        return {
            "text": "Here are similar options — click any one to view details.",
            "intent": "similar_results",
            "cards": cards,
            "_choice_ids": ids,
            "_choice_mode": "info",
        }

    def _build_cards_from_matches(self, matches: List[Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
        cards: List[Dict[str, Any]] = []
        ids: List[str] = []
        for idx, r in enumerate(matches[:8], start=1):
            rid = str(getattr(r, "_id", "") or "").strip()
            md = getattr(r, "metadata", None) or {}
            title = md.get("display_name") or md.get("brick_name") or md.get("item_code") or rid or "Brick"
            brand = md.get("brand")
            colour = md.get("colour") or md.get("brick_colour")

            subtitle_bits = []
            if brand:
                subtitle_bits.append(str(brand))
            if colour:
                subtitle_bits.append(str(colour))

            sim = getattr(r, "overall_similarity", None)
            if isinstance(sim, (int, float)):
                subtitle_bits.append(f"{sim * 100:.1f}% match")
            subtitle = " • ".join(subtitle_bits) if subtitle_bits else None

            img_ref = getattr(r, "image_path", None)
            image_url = self._to_uploads_url(img_ref) if img_ref else None
            cards.append({"choice": idx, "title": str(title), "subtitle": subtitle, "image_url": image_url})
            if rid:
                ids.append(rid)
        return cards, ids

    def _tech_specs_for_brick(self, brick: dict) -> Dict[str, Any]:
        md = brick.get("metadata") or {}
        name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or "This brick"

                                                                 
        known = []
        for k in ("material", "format", "size", "dimensions", "finish", "texture", "weight", "strength"):
            v = md.get(k)
            if v:
                known.append(f"{k.title()}: {v}")

        if known:
            grounded = name + "\n" + "\n".join(known)
        else:
            grounded = name + "\nNo technical spec fields are stored for this item in the catalog yet."

        messages = [
            {"role": "system", "content": self._system_prompt()},
            {"role": "system", "content": "CATALOG CONTEXT (authoritative):\n" + grounded},
            {
                "role": "user",
                "content": "Summarize the technical specs from the catalog context. If specs are missing, say what's missing and ask what spec they need.",
            },
        ]
        text = self._llm_chat(messages, max_tokens=240, temperature=0.4)
        return {"text": text or grounded, "intent": "specs"}

    def _advice_for_brick(self, brick: dict) -> Dict[str, Any]:
        md = brick.get("metadata") or {}
        name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or "This brick"
        brand = md.get("brand")
        colour = md.get("colour") or md.get("brick_colour")
        texture = md.get("texture")

        bits = [f"Brick: {name}"]
        if brand:
            bits.append(f"Brand: {brand}")
        if colour:
            bits.append(f"Colour: {colour}")
        if texture:
            bits.append(f"Texture: {texture}")
        bits.append("Note: Only the above fields are confirmed from the catalog.")
        grounded = "\n".join(bits)

        messages = [
            {"role": "system", "content": self._system_prompt()},
            {"role": "system", "content": "CATALOG CONTEXT (authoritative):\n" + grounded},
            {
                "role": "user",
                "content": "Give high-level, general guidance on suitable project uses for this kind of brick based on the context. Clearly label it as general guidance and avoid claiming product-specific certified properties.",
            },
        ]
        text = self._llm_chat(messages, max_tokens=260, temperature=0.6)
        return {"text": text or (grounded + "\n\nGeneral guidance: Without full spec data, I can only suggest typical uses. If you tell me the project type and exposure (coastal/frost), I’ll narrow it down."), "intent": "advice"}

    def _compare_two_bricks(self, left: dict, right: dict) -> Dict[str, Any]:
        if self.analyzer is None:
            return {"text": "Comparison isn’t available right now (analyzer not initialized).", "intent": "compare"}

        left_img_ref = left.get("image_path")
        right_img_ref = right.get("image_path")
        if not left_img_ref or not right_img_ref:
            return {"text": "I can’t compare those because one of them is missing an image.", "intent": "compare"}

        try:
            li = load_image(left_img_ref, blob=self.blob, uploads_container=self.uploads_container, upload_folder=self.upload_folder)
            ri = load_image(right_img_ref, blob=self.blob, uploads_container=self.uploads_container, upload_folder=self.upload_folder)
            colour = float(self.analyzer.color_similarity_ab(li, ri))
            texture = float(self.analyzer.texture_similarity_relief(li, ri))
            defects = float(self.analyzer.discriminative_similarity(li, ri))
            overall = 0.65 * colour + 0.25 * texture + 0.10 * defects
        except Exception as e:
            return {"text": f"I couldn’t run the comparison right now: {e}", "intent": "compare"}

        lmd = left.get("metadata") or {}
        rmd = right.get("metadata") or {}
        lname = lmd.get("display_name") or lmd.get("brick_name") or lmd.get("item_code") or str(left.get("brick_id") or left.get("_id") or "Left")
        rname = rmd.get("display_name") or rmd.get("brick_name") or rmd.get("item_code") or str(right.get("brick_id") or right.get("_id") or "Right")

        grounded = (
            f"Comparison\n"
            f"Left: {lname}\n"
            f"Right: {rname}\n\n"
            f"Overall: {overall * 100:.1f}%\n"
            f"Colour: {colour * 100:.1f}%\n"
            f"Texture: {texture * 100:.1f}%\n"
            f"Defects: {defects * 100:.1f}%"
        )

        messages = [
            {"role": "system", "content": self._system_prompt()},
            {"role": "system", "content": "COMPARISON CONTEXT (authoritative):\n" + grounded},
            {"role": "user", "content": "Explain the comparison in a friendly way and suggest next steps (view similar options, check pricing, or open the Compare page for visuals)."},
        ]
        text = self._llm_chat(messages, max_tokens=240, temperature=0.5)
        return {"text": text or grounded, "intent": "compare"}

    def _pricing_for_brick(self, brick: dict, bu_code: Optional[str]) -> Dict[str, Any]:
        brick_id = str(brick.get("brick_id") or brick.get("_id") or "").strip()
        md = brick.get("metadata") or {}
        display_name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or brick_id or "(unknown)"

        pricing_doc = get_brick_pricing(brick_id) if brick_id else None
        band_asps = (pricing_doc or {}).get("band_asps") or {}

        if bu_code:
            bu = get_bu_pricing(bu_code, brick)
            band = bu.get("bu_band")
            asp = bu.get("bu_asp")
            if asp is not None:
                extra = "" if bu.get("in_region") else " (out-of-region/default band)"
                grounded = f"Brick: {display_name}\nBU: {bu_code}\nBand: {band}{extra}\nASP: {asp}"
                messages = [
                    {"role": "system", "content": self._system_prompt()},
                    {"role": "system", "content": "PRICING CONTEXT (authoritative):\n" + grounded},
                    {"role": "user", "content": "Answer in a friendly, natural tone. Keep it short."},
                ]
                text = self._llm_chat(messages, max_tokens=160, temperature=0.4)
                return {"text": text or grounded, "intent": "pricing"}

            if band_asps:
                available = ", ".join(sorted(str(k) for k in band_asps.keys()))
                return {
                    "text": f"{display_name}\nBU {bu_code}: band {band}, but no ASP is stored for that band. Available bands for this brick: {available}.",
                    "intent": "pricing",
                }
            return {"text": f"{display_name}\nNo pricing is stored for this brick yet.", "intent": "pricing"}

        if band_asps:
            available = ", ".join(sorted(str(k) for k in band_asps.keys()))
            return {
                "text": f"{display_name}\nPricing bands available in the catalog: {available}. Set your BU in Profile to see your BU-specific band/ASP.",
                "intent": "pricing",
            }
        return {
            "text": f"{display_name}\nNo pricing is stored for this brick yet. Set your BU in Profile and/or ingest pricing bands for this item.",
            "intent": "pricing",
        }

    def _looks_brick_related(self, msg_lower: str) -> bool:
                                                                                  
        keywords = (
            "brick",
            "bricks",
            "paver",
            "block",
            "clay",
            "colour",
            "color",
            "texture",
            "factory",
            "band",
            "asp",
            "price",
            "pricing",
            "compare",
            "similar",
            "catalog",
            "item code",
        )
        return any(k in msg_lower for k in keywords)

    def _find_bricks(self, query: str, limit: int = 5) -> List[dict]:
        q = (query or "").strip()
        if not q:
            return []

                                                  
        brick_id = self._extract_probable_id(q)
        db = get_db()
        coll = db.bricks

        if brick_id:
            doc = coll.find_one({"$or": [{"brick_id": brick_id}, {"_id": brick_id}]})
            if doc:
                return [doc]

                                                              
        rx = self._query_to_regex(q)
        if not rx:
            return []

        fields = [
            "metadata.display_name",
            "metadata.brick_name",
            "metadata.item_code",
            "metadata.item_number",
            "metadata.brand",
            "metadata.factory",
            "metadata.colour",
            "metadata.brick_colour",
            "manufacturer",
        ]
        or_terms = [{f: {"$regex": rx, "$options": "i"}} for f in fields]
        cursor = coll.find({"dataset": "catalog", "$or": or_terms}).limit(int(limit))
        return list(cursor)

    def _format_disambiguation(self, matches: List[dict], prefix: str = "I found multiple matches:") -> tuple[str, List[str]]:
        lines = [prefix]
        ids: List[str] = []

        for d in matches[:5]:
            md = d.get("metadata") or {}
            bid = str(d.get("brick_id") or d.get("_id") or "").strip()
            name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or bid
            brand = md.get("brand")
            colour = md.get("colour") or md.get("brick_colour")
            bits = [str(name)]
            if brand:
                bits.append(str(brand))
            if colour:
                bits.append(str(colour))
            label = " — ".join(bits)
            lines.append(f"- {label}")
            if bid:
                ids.append(bid)

        lines.append("Click a brick card to choose one.")
        return "\n".join(lines), ids

    def _build_cards(self, matches: List[dict]) -> List[Dict[str, Any]]:
        cards: List[Dict[str, Any]] = []
        for idx, d in enumerate(matches[:5], start=1):
            md = d.get("metadata") or {}
            bid = str(d.get("brick_id") or d.get("_id") or "").strip()
            title = md.get("display_name") or md.get("brick_name") or md.get("item_code") or bid or "Brick"
            brand = md.get("brand")
            colour = md.get("colour") or md.get("brick_colour")

            subtitle_bits = []
            if brand:
                subtitle_bits.append(str(brand))
            if colour:
                subtitle_bits.append(str(colour))
            subtitle = " • ".join(subtitle_bits) if subtitle_bits else None

            img_ref = d.get("image_path") or md.get("image_path")
            image_url = self._to_uploads_url(img_ref) if img_ref else None

            cards.append(
                {
                    "choice": idx,
                    "title": str(title),
                    "subtitle": str(subtitle) if subtitle else None,
                    "image_url": image_url,
                }
            )
        return cards

    def _to_uploads_url(self, image_path: str) -> Optional[str]:
        if not image_path:
            return None
        p = str(image_path).lstrip("/\\")
                                            
        return "/uploads/" + quote(p.replace("\\", "/"), safe="/")

    def _resolve_choice_from_choice_ids(self, user_text: str, choice_ids: Optional[List[str]]) -> Optional[dict]:
        if not choice_ids:
            return None
        m = re.match(r"^\s*(?:option\s*)?(\d+)\s*$", user_text or "", flags=re.I)
        if not m:
            return None
        try:
            choice = int(m.group(1))
        except Exception:
            return None
        if choice <= 0 or choice > len(choice_ids):
            return None

        brick_id = choice_ids[choice - 1]
        try:
            db = get_db()
            return db.bricks.find_one({"$or": [{"brick_id": brick_id}, {"_id": brick_id}]})
        except Exception:
            return None

    def _resolve_choice_from_history(self, user_text: str, conversation_history: List[Dict[str, str]]) -> Optional[dict]:
        m = re.match(r"^\s*(?:option\s*)?(\d+)\s*$", user_text or "", flags=re.I)
        if not m:
            return None
        try:
            choice = int(m.group(1))
        except Exception:
            return None
        if choice <= 0:
            return None

                                                                
        last_assistant = None
        for h in reversed(conversation_history or []):
            if h.get("role") == "assistant" and h.get("content"):
                last_assistant = str(h.get("content"))
                break
        if not last_assistant:
            return None

        ids = re.findall(r"\bid:\s*([A-Fa-f0-9]{16,})\b", last_assistant)
        if not ids or choice > len(ids):
            return None

        brick_id = ids[choice - 1]
        try:
            db = get_db()
            return db.bricks.find_one({"$or": [{"brick_id": brick_id}, {"_id": brick_id}]})
        except Exception:
            return None

    def _format_brick_summary(self, brick: dict, bu_code: Optional[str] = None) -> str:
        md = brick.get("metadata") or {}

        name = md.get("display_name") or md.get("brick_name") or md.get("item_code") or "(unknown)"
        brand = md.get("brand")
        colour = md.get("colour") or md.get("brick_colour")
        item_code = md.get("item_code")
        factory_code = md.get("factory_code") or brick.get("factory_code")

        lines = [name]
        if item_code:
            lines.append(f"Item code: {item_code}")
        if brand:
            lines.append(f"Brand: {brand}")
        if colour:
            lines.append(f"Colour: {colour}")
        if factory_code:
            lines.append(f"Factory: {factory_code}")

                                                      
        if bu_code:
            try:
                bu = get_bu_pricing(bu_code, brick)
                if bu.get("bu_band") and bu.get("bu_asp") is not None:
                    extra = "" if bu.get("in_region") else " (out-of-region/default band)"
                    lines.append(f"BU {bu_code} price: band {bu.get('bu_band')}{extra}, ASP {bu.get('bu_asp')}")
            except Exception:
                pass

        lines.append("Want me to show similar options, pricing, or specs?")
        return "\n".join(lines)

    def _extract_probable_id(self, text: str) -> Optional[str]:
                                                                          
        tokens = re.findall(r"[A-Za-z0-9\-]{6,}", text or "")
        if not tokens:
            return None
                                                  
        tokens.sort(key=len, reverse=True)
        return tokens[0]

    def _query_to_regex(self, query: str) -> Optional[str]:
        q = (query or "").strip()
        if len(q) < 2:
            return None

                                                                    
                                                              
        q = re.sub(r"[^A-Za-z0-9]+", " ", q)
        q = re.sub(r"\s+", " ", q).strip().lower()
        if len(q) < 2:
            return None

        stop = {
            "what",
            "is",
            "the",
            "a",
            "an",
            "of",
            "for",
            "please",
            "show",
            "me",
            "tell",
            "about",
            "price",
            "pricing",
            "cost",
            "asp",
        }
        tokens = [t for t in q.split(" ") if len(t) >= 3 and t not in stop]
        if not tokens:
            return None

                                                 
                                                                 
        return ".*".join(re.escape(t) for t in tokens)
