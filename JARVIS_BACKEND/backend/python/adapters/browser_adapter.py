from __future__ import annotations

import webbrowser


class BrowserAdapter:
    """
    Browser-specific actions that abstract webbrowser behavior behind a stable adapter.
    """

    @staticmethod
    def open_url(url: str, *, new_tab: bool = True) -> dict:
        target = str(url or "").strip()
        if not target:
            return {"status": "error", "message": "url is required"}
        if not target.startswith(("http://", "https://")):
            target = f"https://{target}"

        webbrowser.open(target, new=2 if new_tab else 1)
        return {"status": "success", "url": target, "adapter": "browser"}

