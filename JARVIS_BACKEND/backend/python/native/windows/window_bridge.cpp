#include <windows.h>

#include <algorithm>
#include <cstdio>
#include <cwctype>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

#include "window_bridge.hpp"

namespace jarvis::native {
namespace {

struct WindowSnapshot {
    long long hwnd = 0;
    long long owner_hwnd = 0;
    long pid = 0;
    std::string title;
    std::string exe;
    std::string process_name;
    std::string class_name;
    bool visible = false;
    bool enabled = false;
    bool minimized = false;
    bool maximized = false;
    bool is_foreground = false;
    int left = 0;
    int top = 0;
    int right = 0;
    int bottom = 0;
};

std::string wide_to_utf8(const std::wstring& value) {
    if (value.empty()) {
        return {};
    }
    const int required = WideCharToMultiByte(
        CP_UTF8,
        0,
        value.c_str(),
        static_cast<int>(value.size()),
        nullptr,
        0,
        nullptr,
        nullptr
    );
    if (required <= 0) {
        return {};
    }
    std::string output(static_cast<std::size_t>(required), '\0');
    WideCharToMultiByte(
        CP_UTF8,
        0,
        value.c_str(),
        static_cast<int>(value.size()),
        output.data(),
        required,
        nullptr,
        nullptr
    );
    return output;
}

std::wstring utf8_to_wide(const std::string& value) {
    if (value.empty()) {
        return {};
    }
    const int required = MultiByteToWideChar(
        CP_UTF8,
        0,
        value.c_str(),
        static_cast<int>(value.size()),
        nullptr,
        0
    );
    if (required <= 0) {
        return {};
    }
    std::wstring output(static_cast<std::size_t>(required), L'\0');
    MultiByteToWideChar(
        CP_UTF8,
        0,
        value.c_str(),
        static_cast<int>(value.size()),
        output.data(),
        required
    );
    return output;
}

std::wstring basename_from_path(const std::wstring& path) {
    if (path.empty()) {
        return {};
    }
    const std::size_t slash = path.find_last_of(L"\\/");
    if (slash == std::wstring::npos) {
        return path;
    }
    return path.substr(slash + 1);
}

std::string json_escape(const std::string& value) {
    std::ostringstream output;
    for (const unsigned char ch : value) {
        switch (ch) {
            case '\"':
                output << "\\\"";
                break;
            case '\\':
                output << "\\\\";
                break;
            case '\b':
                output << "\\b";
                break;
            case '\f':
                output << "\\f";
                break;
            case '\n':
                output << "\\n";
                break;
            case '\r':
                output << "\\r";
                break;
            case '\t':
                output << "\\t";
                break;
            default:
                if (ch < 0x20) {
                    char buffer[7];
                    std::snprintf(buffer, sizeof(buffer), "\\u%04x", ch);
                    output << buffer;
                } else {
                    output << static_cast<char>(ch);
                }
                break;
        }
    }
    return output.str();
}

std::string build_error_json(const std::string& message) {
    std::ostringstream output;
    output << "{\"status\":\"error\",\"backend\":\"cpp_cython\",\"message\":\"" << json_escape(message) << "\"}";
    return output.str();
}

std::string snapshot_to_json(const WindowSnapshot& snapshot) {
    std::ostringstream output;
    output << "{"
           << "\"hwnd\":" << snapshot.hwnd << ","
           << "\"owner_hwnd\":" << snapshot.owner_hwnd << ","
           << "\"pid\":" << snapshot.pid << ","
           << "\"title\":\"" << json_escape(snapshot.title) << "\","
           << "\"exe\":\"" << json_escape(snapshot.exe) << "\","
           << "\"process_name\":\"" << json_escape(snapshot.process_name) << "\","
           << "\"class_name\":\"" << json_escape(snapshot.class_name) << "\","
           << "\"visible\":" << (snapshot.visible ? "true" : "false") << ","
           << "\"enabled\":" << (snapshot.enabled ? "true" : "false") << ","
           << "\"minimized\":" << (snapshot.minimized ? "true" : "false") << ","
           << "\"maximized\":" << (snapshot.maximized ? "true" : "false") << ","
           << "\"is_foreground\":" << (snapshot.is_foreground ? "true" : "false") << ","
           << "\"left\":" << snapshot.left << ","
           << "\"top\":" << snapshot.top << ","
           << "\"right\":" << snapshot.right << ","
           << "\"bottom\":" << snapshot.bottom
           << "}";
    return output.str();
}

bool collect_window_snapshot(HWND hwnd, HWND foreground, WindowSnapshot& snapshot) {
    if (hwnd == nullptr || !IsWindow(hwnd) || !IsWindowVisible(hwnd)) {
        return false;
    }

    wchar_t title_buffer[1024];
    const int title_size = GetWindowTextW(hwnd, title_buffer, static_cast<int>(std::size(title_buffer)));
    if (title_size <= 0) {
        return false;
    }

    wchar_t class_buffer[256];
    const int class_size = GetClassNameW(hwnd, class_buffer, static_cast<int>(std::size(class_buffer)));
    RECT rect{};
    if (!GetWindowRect(hwnd, &rect)) {
        rect = RECT{0, 0, 0, 0};
    }

    DWORD pid = 0;
    GetWindowThreadProcessId(hwnd, &pid);

    std::wstring exe_path;
    if (pid != 0) {
        HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
        if (process != nullptr) {
            wchar_t exe_buffer[32768];
            DWORD exe_size = static_cast<DWORD>(std::size(exe_buffer));
            if (QueryFullProcessImageNameW(process, 0, exe_buffer, &exe_size)) {
                exe_path.assign(exe_buffer, exe_size);
            }
            CloseHandle(process);
        }
    }

    snapshot.hwnd = static_cast<long long>(reinterpret_cast<intptr_t>(hwnd));
    snapshot.owner_hwnd = static_cast<long long>(reinterpret_cast<intptr_t>(GetWindow(hwnd, GW_OWNER)));
    snapshot.pid = static_cast<long>(pid);
    snapshot.title = wide_to_utf8(std::wstring(title_buffer, static_cast<std::size_t>(title_size)));
    snapshot.exe = wide_to_utf8(exe_path);
    snapshot.process_name = wide_to_utf8(basename_from_path(exe_path));
    snapshot.class_name = wide_to_utf8(
        class_size > 0 ? std::wstring(class_buffer, static_cast<std::size_t>(class_size)) : std::wstring()
    );
    snapshot.visible = true;
    snapshot.enabled = IsWindowEnabled(hwnd) != FALSE;
    snapshot.minimized = IsIconic(hwnd) != FALSE;
    snapshot.maximized = IsZoomed(hwnd) != FALSE;
    snapshot.is_foreground = hwnd == foreground;
    snapshot.left = rect.left;
    snapshot.top = rect.top;
    snapshot.right = rect.right;
    snapshot.bottom = rect.bottom;
    return true;
}

std::wstring to_lower_copy(const std::wstring& value) {
    std::wstring output = value;
    std::transform(output.begin(), output.end(), output.begin(), [](wchar_t ch) {
        return static_cast<wchar_t>(std::towlower(ch));
    });
    return output;
}

struct EnumWindowsContext {
    std::vector<WindowSnapshot>* rows = nullptr;
    HWND foreground = nullptr;
    int limit = 120;
};

BOOL CALLBACK EnumWindowsListProc(HWND hwnd, LPARAM lparam) {
    auto* context = reinterpret_cast<EnumWindowsContext*>(lparam);
    if (context == nullptr || context->rows == nullptr) {
        return FALSE;
    }
    if (static_cast<int>(context->rows->size()) >= context->limit) {
        return FALSE;
    }

    WindowSnapshot snapshot;
    if (collect_window_snapshot(hwnd, context->foreground, snapshot)) {
        context->rows->push_back(snapshot);
    }
    return TRUE;
}

struct FindWindowContext {
    std::wstring needle;
    HWND match = nullptr;
};

BOOL CALLBACK EnumWindowsFindProc(HWND hwnd, LPARAM lparam) {
    auto* context = reinterpret_cast<FindWindowContext*>(lparam);
    if (context == nullptr) {
        return FALSE;
    }
    if (!IsWindowVisible(hwnd)) {
        return TRUE;
    }

    wchar_t title_buffer[1024];
    const int title_size = GetWindowTextW(hwnd, title_buffer, static_cast<int>(std::size(title_buffer)));
    if (title_size <= 0) {
        return TRUE;
    }
    const std::wstring title(title_buffer, static_cast<std::size_t>(title_size));
    if (to_lower_copy(title).find(context->needle) != std::wstring::npos) {
        context->match = hwnd;
        return FALSE;
    }
    return TRUE;
}

}  // namespace

std::string list_windows_json(int limit) {
    const int safe_limit = std::max(1, std::min(limit, 500));
    const HWND foreground = GetForegroundWindow();

    std::vector<WindowSnapshot> rows;
    rows.reserve(static_cast<std::size_t>(safe_limit));

    EnumWindowsContext context{&rows, foreground, safe_limit};
    EnumWindows(EnumWindowsListProc, reinterpret_cast<LPARAM>(&context));

    std::ostringstream output;
    output << "{\"status\":\"success\",\"backend\":\"cpp_cython\",\"count\":" << rows.size() << ",\"windows\":[";
    for (std::size_t index = 0; index < rows.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << snapshot_to_json(rows[index]);
    }
    output << "]}";
    return output.str();
}

std::string active_window_json() {
    const HWND foreground = GetForegroundWindow();
    if (foreground == nullptr) {
        return build_error_json("No foreground window found.");
    }

    WindowSnapshot snapshot;
    if (!collect_window_snapshot(foreground, foreground, snapshot)) {
        return build_error_json("Unable to inspect the active window.");
    }

    std::ostringstream output;
    output << "{\"status\":\"success\",\"backend\":\"cpp_cython\",\"window\":" << snapshot_to_json(snapshot) << "}";
    return output.str();
}

std::string focus_window_json(const std::string& title_contains_utf8, long long hwnd_value) {
    HWND target = nullptr;
    if (hwnd_value > 0) {
        auto* candidate = reinterpret_cast<HWND>(static_cast<intptr_t>(hwnd_value));
        if (IsWindow(candidate)) {
            target = candidate;
        }
    }

    if (target == nullptr && !title_contains_utf8.empty()) {
        FindWindowContext context{to_lower_copy(utf8_to_wide(title_contains_utf8)), nullptr};
        EnumWindows(EnumWindowsFindProc, reinterpret_cast<LPARAM>(&context));
        target = context.match;
    }

    if (target == nullptr) {
        return build_error_json("Window not found.");
    }

    if (IsIconic(target)) {
        ShowWindow(target, SW_RESTORE);
    } else {
        ShowWindow(target, SW_SHOW);
    }
    BringWindowToTop(target);
    const BOOL focus_result = SetForegroundWindow(target);

    const HWND foreground = GetForegroundWindow();
    WindowSnapshot snapshot;
    if (!collect_window_snapshot(target, foreground, snapshot)) {
        return build_error_json("Window was focused but could not be inspected.");
    }

    std::ostringstream output;
    output << "{\"status\":\"success\",\"backend\":\"cpp_cython\",\"focus_applied\":"
           << ((focus_result != FALSE || foreground == target) ? "true" : "false")
           << ",\"window\":" << snapshot_to_json(snapshot) << "}";
    return output.str();
}

}  // namespace jarvis::native
