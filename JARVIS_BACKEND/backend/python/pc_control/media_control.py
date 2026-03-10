from typing import Any, Dict

try:
    from winsdk.windows.media.control import (  # type: ignore
        GlobalSystemMediaTransportControlsSessionManager as MediaManager,
    )
    from winsdk.windows.media.control import (  # type: ignore
        GlobalSystemMediaTransportControlsSessionPlaybackStatus as PlaybackStatus,
    )
except Exception:  # noqa: BLE001
    MediaManager = None  # type: ignore
    PlaybackStatus = None  # type: ignore


class MediaController:
    async def get_session(self):
        if MediaManager is None:
            return None
        mgr = await MediaManager.request_async()
        return mgr.get_current_session()

    async def get_media_info(self) -> Dict[str, Any]:
        try:
            session = await self.get_session()
            if not session:
                return {"status": "error", "message": "No media session"}
            info = await session.try_get_media_properties_async()
            return {
                "status": "success",
                "title": info.title,
                "artist": info.artist,
                "album": info.album_title,
                "genres": info.genres,
                "playback_status": session.get_playback_info().playback_status.name,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    async def play_pause(self) -> Dict[str, Any]:
        try:
            session = await self.get_session()
            if not session:
                return {"status": "error", "message": "No media session"}
            status = session.get_playback_info().playback_status
            if PlaybackStatus is not None and status == PlaybackStatus.PLAYING:
                await session.try_pause_async()
            else:
                await session.try_play_async()
            return {"status": "success"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    async def play(self) -> Dict[str, Any]:
        try:
            session = await self.get_session()
            if not session:
                return {"status": "error", "message": "No media session"}
            await session.try_play_async()
            return {"status": "success"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    async def pause(self) -> Dict[str, Any]:
        try:
            session = await self.get_session()
            if not session:
                return {"status": "error", "message": "No media session"}
            await session.try_pause_async()
            return {"status": "success"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    async def stop(self) -> Dict[str, Any]:
        try:
            session = await self.get_session()
            if not session:
                return {"status": "error", "message": "No media session"}
            await session.try_stop_async()
            return {"status": "success"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    async def next_track(self) -> Dict[str, Any]:
        try:
            session = await self.get_session()
            if not session:
                return {"status": "error", "message": "No media session"}
            await session.try_skip_next_async()
            return {"status": "success"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    async def previous_track(self) -> Dict[str, Any]:
        try:
            session = await self.get_session()
            if not session:
                return {"status": "error", "message": "No media session"}
            await session.try_skip_previous_async()
            return {"status": "success"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
