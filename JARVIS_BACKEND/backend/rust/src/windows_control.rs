use serde_json::json;
use windows::Win32::Foundation::{BOOL, HWND, LPARAM, RECT};
use windows::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetClassNameW, GetForegroundWindow, GetWindow, GetWindowInfo, GetWindowRect,
    GetWindowTextW, GetWindowThreadProcessId, IsIconic, IsWindowVisible,
    IsZoomed, WINDOWINFO, GW_OWNER,
};

use std::ffi::OsString;
use std::os::windows::ffi::OsStringExt;

pub struct WindowsControl;

impl WindowsControl {
    fn owner_chain_metrics(hwnd: HWND) -> (isize, u32) {
        let mut owner = unsafe { GetWindow(hwnd, GW_OWNER) };
        if owner.0 == 0 {
            return (hwnd.0, 0);
        }
        let mut root_owner = owner;
        let mut depth = 0_u32;
        let mut guard = 0_u32;
        while owner.0 != 0 && guard < 32 {
            depth += 1;
            root_owner = owner;
            let next_owner = unsafe { GetWindow(owner, GW_OWNER) };
            if next_owner.0 == owner.0 {
                break;
            }
            owner = next_owner;
            guard += 1;
        }
        (root_owner.0, depth)
    }

    pub fn get_active_window() -> anyhow::Result<serde_json::Value> {
        unsafe {
            let hwnd = GetForegroundWindow();
            if hwnd.0 == 0 {
                anyhow::bail!("No active window");
            }
            Ok(Self::inspect_window(hwnd)?)
        }
    }

    pub fn list_windows() -> anyhow::Result<Vec<serde_json::Value>> {
        let mut windows: Vec<serde_json::Value> = vec![];

        unsafe {
            EnumWindows(
                Some(Self::enum_callback),
                LPARAM(&mut windows as *mut _ as isize),
            )
            .map_err(|err| anyhow::anyhow!("EnumWindows failed: {err}"))?;
        }

        Ok(windows)
    }

    unsafe extern "system" fn enum_callback(hwnd: HWND, lparam: LPARAM) -> BOOL {
        let vec_ptr = lparam.0 as *mut Vec<serde_json::Value>;
        if let Some(vec) = vec_ptr.as_mut() {
            if let Ok(info) = WindowsControl::inspect_window(hwnd) {
                let title = info
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let visible = info
                    .get("visible")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                if visible && !title.is_empty() {
                    vec.push(info);
                }
            }
        }
        BOOL(1)
    }

    fn get_window_title(hwnd: HWND) -> String {
        let mut buf = [0u16; 512];
        unsafe {
            let len = GetWindowTextW(hwnd, &mut buf);
            OsString::from_wide(&buf[..len as usize])
                .to_string_lossy()
                .to_string()
        }
    }

    fn get_class_name(hwnd: HWND) -> String {
        let mut buf = [0u16; 256];
        unsafe {
            let len = GetClassNameW(hwnd, &mut buf);
            OsString::from_wide(&buf[..len as usize])
                .to_string_lossy()
                .to_string()
        }
    }

    fn get_window_rect(hwnd: HWND) -> anyhow::Result<(i32, i32, i32, i32)> {
        let mut rect = RECT::default();
        unsafe {
            if GetWindowRect(hwnd, &mut rect).is_err() {
                anyhow::bail!("GetWindowRect failed");
            }
        }
        Ok((rect.left, rect.top, rect.right, rect.bottom))
    }

    fn get_process_id(hwnd: HWND) -> u32 {
        let mut pid = 0;
        unsafe {
            GetWindowThreadProcessId(hwnd, Some(&mut pid));
        }
        pid
    }

    fn get_window_info(hwnd: HWND) -> anyhow::Result<WINDOWINFO> {
        let mut info = WINDOWINFO::default();
        info.cbSize = std::mem::size_of::<WINDOWINFO>() as u32;
        unsafe {
            if GetWindowInfo(hwnd, &mut info).is_err() {
                anyhow::bail!("GetWindowInfo failed");
            }
        }
        Ok(info)
    }

    fn normalize_text(value: &str) -> String {
        value
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_ascii_lowercase()
    }

    fn tokenize(value: &str) -> Vec<String> {
        let mut normalized = Self::normalize_text(value);
        for token in ["|", "-", "_", "/", "\\", "(", ")", "[", "]", "{", "}", ".", ",", ":"] {
            normalized = normalized.replace(token, " ");
        }
        normalized
            .split_whitespace()
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect()
    }

    fn infer_app_name(title: &str, class_name: &str) -> String {
        let title_tokens = Self::tokenize(title);
        if let Some(first) = title_tokens.first() {
            return first.to_string();
        }
        let class_tokens = Self::tokenize(class_name);
        class_tokens
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string())
    }

    fn build_window_signature(
        title: &str,
        class_name: &str,
        left: i32,
        top: i32,
        right: i32,
        bottom: i32,
    ) -> String {
        let app_name = Self::infer_app_name(title, class_name);
        let title_fragment = {
            let tokens = Self::tokenize(title);
            if tokens.is_empty() {
                "untitled".to_string()
            } else {
                tokens.into_iter().take(6).collect::<Vec<_>>().join("_")
            }
        };
        let class_fragment = {
            let normalized = Self::normalize_text(class_name).replace(' ', "_");
            if normalized.is_empty() {
                "window".to_string()
            } else {
                normalized
            }
        };
        let width = (right - left).max(0);
        let height = (bottom - top).max(0);
        format!("{app_name}|{class_fragment}|{width}x{height}|{title_fragment}")
    }

    fn infer_surface_hints(title: &str, class_name: &str, app_name: &str) -> serde_json::Value {
        let title_norm = Self::normalize_text(title);
        let class_norm = Self::normalize_text(class_name);
        let app_norm = Self::normalize_text(app_name);
        let combined = [title_norm.as_str(), class_norm.as_str(), app_norm.as_str()]
            .iter()
            .filter(|value| !value.is_empty())
            .cloned()
            .collect::<Vec<_>>()
            .join(" ");
        let dialog_like = class_name.contains("#32770")
            || combined.contains("dialog")
            || combined.contains("properties")
            || combined.contains("warning")
            || combined.contains("confirm")
            || combined.contains("permission")
            || combined.contains("credential");
        let browser_like = combined.contains("chrome")
            || combined.contains("edge")
            || combined.contains("firefox")
            || combined.contains("browser");
        let settings_like = combined.contains("settings") || combined.contains("control panel");
        let file_manager_like = combined.contains("explorer") || combined.contains("items view");
        let admin_like = combined.contains("task manager")
            || combined.contains("device manager")
            || combined.contains("event viewer")
            || combined.contains("services")
            || combined.contains("registry");
        let pane_like = combined.contains("pane")
            || combined.contains("sidebar")
            || combined.contains("navigation")
            || combined.contains("reading pane");
        json!({
            "dialog_like": dialog_like,
            "browser_like": browser_like,
            "settings_like": settings_like,
            "file_manager_like": file_manager_like,
            "admin_like": admin_like,
            "pane_like": pane_like,
        })
    }

    fn inspect_window(hwnd: HWND) -> anyhow::Result<serde_json::Value> {
        let title = Self::get_window_title(hwnd);
        let (left, top, right, bottom) = Self::get_window_rect(hwnd)?;
        let pid = Self::get_process_id(hwnd);
        let info = Self::get_window_info(hwnd)?;
        let class_name = Self::get_class_name(hwnd);
        let visible = unsafe { IsWindowVisible(hwnd).as_bool() };
        // The current windows crate surface in this repo does not expose IsWindowEnabled,
        // so we keep the field but treat visible top-level windows as actionable by default.
        let enabled = visible;
        let minimized = unsafe { IsIconic(hwnd).as_bool() };
        let maximized = unsafe { IsZoomed(hwnd).as_bool() };
        let foreground_hwnd = unsafe { GetForegroundWindow() };
        let owner_hwnd = unsafe { GetWindow(hwnd, GW_OWNER) };
        let (root_owner_hwnd, owner_chain_depth) = Self::owner_chain_metrics(hwnd);
        let app_name = Self::infer_app_name(&title, &class_name);
        let surface_hints = Self::infer_surface_hints(&title, &class_name, &app_name);
        let window_signature =
            Self::build_window_signature(&title, &class_name, left, top, right, bottom);

        Ok(json!({
            "hwnd": hwnd.0,
            "owner_hwnd": owner_hwnd.0,
            "root_owner_hwnd": root_owner_hwnd,
            "owner_chain_depth": owner_chain_depth,
            "title": title,
            "process_id": pid,
            "class_name": class_name,
            "app_name": app_name,
            "visible": visible,
            "enabled": enabled,
            "minimized": minimized,
            "maximized": maximized,
            "is_foreground": foreground_hwnd.0 == hwnd.0,
            "window_signature": window_signature,
            "surface_hints": surface_hints,
            "geometry": {
                "x": left,
                "y": top,
                "width": right - left,
                "height": bottom - top
            },
            "window_info": {
                "style": info.dwStyle.0,
                "ex_style": info.dwExStyle.0,
                "border": {
                    "left": info.rcWindow.left,
                    "top": info.rcWindow.top,
                    "right": info.rcWindow.right,
                    "bottom": info.rcWindow.bottom
                }
            }
        }))
    }
}
