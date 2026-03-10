use serde_json::json;
use windows::Win32::Foundation::{BOOL, HWND, LPARAM, RECT};
use windows::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetForegroundWindow, GetWindowInfo, GetWindowRect, GetWindowTextW,
    GetWindowThreadProcessId, WINDOWINFO,
};

use std::ffi::OsString;
use std::os::windows::ffi::OsStringExt;

pub struct WindowsControl;

impl WindowsControl {
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
                vec.push(info);
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

    fn inspect_window(hwnd: HWND) -> anyhow::Result<serde_json::Value> {
        let title = Self::get_window_title(hwnd);
        let (left, top, right, bottom) = Self::get_window_rect(hwnd)?;
        let pid = Self::get_process_id(hwnd);
        let info = Self::get_window_info(hwnd)?;

        Ok(json!({
            "hwnd": hwnd.0,
            "title": title,
            "process_id": pid,
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
