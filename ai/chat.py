import logging

from flask import Blueprint, jsonify, request, g, session

from ai.brick_assistant import BrickAssistant

logger = logging.getLogger(__name__)

chat_bp = Blueprint("assistant", __name__, url_prefix="/assistant")

_assistant: BrickAssistant | None = None


def init_chat(assistant: BrickAssistant) -> None:
    global _assistant
    _assistant = assistant


@chat_bp.route("", methods=["GET"], endpoint="assistant_page")
def assistant_page():
    from flask import render_template

    return render_template("assistant.html")


@chat_bp.route("/chat", methods=["POST"])
def assistant_chat():
    if _assistant is None:
        return jsonify({"text": "Assistant not initialized."}), 500

    data = request.get_json(force=True) or {}
    message = (data.get("message") or "").strip()
    history = data.get("conversation_history") or []
    selection = data.get("selection")

    bu_code = getattr(g, "user_bu_code", None)

    last_choice_ids = session.get("assistant_last_choice_ids")
    last_choice_mode = session.get("assistant_last_choice_mode")
    selected_brick_id = session.get("assistant_selected_brick_id")
    pending_action = session.get("assistant_pending_action")

    try:
        if selection is not None:
                                                       
            resp = _assistant.chat(
                str(selection),
                history,
                bu_code=bu_code,
                choice_ids=last_choice_ids,
                pending_intent=last_choice_mode,
                selected_brick_id=selected_brick_id,
            )
        else:
            resp = _assistant.chat(
                message,
                history,
                bu_code=bu_code,
                choice_ids=last_choice_ids,
                pending_intent=pending_action,
                selected_brick_id=selected_brick_id,
            )

                                                                                  
                                                   
        if isinstance(resp, dict) and resp.get("_choice_ids"):
            session["assistant_last_choice_ids"] = resp.pop("_choice_ids")
            session["assistant_last_choice_mode"] = resp.pop("_choice_mode", None)
        else:
            session.pop("assistant_last_choice_ids", None)
            session.pop("assistant_last_choice_mode", None)

                                                                                             
        if isinstance(resp, dict) and resp.get("_selected_brick_id"):
            session["assistant_selected_brick_id"] = resp.pop("_selected_brick_id")

                                                                          
        if isinstance(resp, dict) and resp.get("_pending_action"):
            session["assistant_pending_action"] = resp.pop("_pending_action")
        else:
            session.pop("assistant_pending_action", None)

                                                                           
        return jsonify(resp)
    except Exception:
        logger.exception("Assistant error")
        return jsonify({"text": "I hit an error. Try a simpler question."}), 500


@chat_bp.route("/health", methods=["GET"])
def assistant_health():
    ok = _assistant is not None
    return jsonify({"ok": ok})
