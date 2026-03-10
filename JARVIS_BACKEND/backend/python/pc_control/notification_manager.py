from typing import Dict, Optional

try:
    import winrt.windows.data.xml.dom as dom  # type: ignore
    import winrt.windows.ui.notifications as notifications  # type: ignore
except Exception:  # noqa: BLE001
    dom = None  # type: ignore
    notifications = None  # type: ignore


class NotificationManager:
    def send_notification(self, title: str, message: str, icon_path: Optional[str] = None) -> Dict[str, str]:
        if dom is None or notifications is None:
            return {"status": "error", "message": "winrt notification dependencies unavailable"}

        template = f"""
        <toast activationType="foreground">
            <visual>
                <binding template="ToastGeneric">
                    <text>{title}</text>
                    <text>{message}</text>
                    {f'<image src="{icon_path}"/>' if icon_path else ''}
                </binding>
            </visual>
        </toast>
        """
        xml = dom.XmlDocument()
        xml.load_xml(template)

        notifier = notifications.ToastNotificationManager.create_toast_notifier("JarvisAI")
        toast = notifications.ToastNotification(xml)
        notifier.show(toast)
        return {"status": "success", "title": title}
