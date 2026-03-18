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
    long long root_owner_hwnd = 0;
    int owner_chain_depth = 0;
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
           << "\"root_owner_hwnd\":" << snapshot.root_owner_hwnd << ","
           << "\"owner_chain_depth\":" << snapshot.owner_chain_depth << ","
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

    HWND owner = GetWindow(hwnd, GW_OWNER);
    int owner_chain_depth = 0;
    HWND root_owner = hwnd;
    HWND current_owner = owner;
    int guard = 0;
    while (current_owner != nullptr && current_owner != HWND(0) && guard < 32) {
        ++owner_chain_depth;
        root_owner = current_owner;
        HWND next_owner = GetWindow(current_owner, GW_OWNER);
        if (next_owner == current_owner) {
            break;
        }
        current_owner = next_owner;
        ++guard;
    }

    snapshot.hwnd = static_cast<long long>(reinterpret_cast<intptr_t>(hwnd));
    snapshot.owner_hwnd = static_cast<long long>(reinterpret_cast<intptr_t>(owner));
    snapshot.root_owner_hwnd = static_cast<long long>(reinterpret_cast<intptr_t>(root_owner));
    snapshot.owner_chain_depth = owner_chain_depth;
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

double substring_match_score(const std::wstring& haystack, const std::wstring& needle) {
    const std::wstring clean_haystack = to_lower_copy(haystack);
    const std::wstring clean_needle = to_lower_copy(needle);
    if (clean_haystack.empty() || clean_needle.empty()) {
        return 0.0;
    }
    if (clean_haystack == clean_needle) {
        return 1.0;
    }
    const std::size_t position = clean_haystack.find(clean_needle);
    if (position == std::wstring::npos) {
        return 0.0;
    }
    const double coverage = static_cast<double>(clean_needle.size()) /
        static_cast<double>(std::max<std::size_t>(1, clean_haystack.size()));
    return std::clamp(coverage + 0.18, 0.38, 0.96);
}

bool snapshot_is_dialog_like(const WindowSnapshot& snapshot) {
    const std::wstring class_name = to_lower_copy(utf8_to_wide(snapshot.class_name));
    const std::wstring title = to_lower_copy(utf8_to_wide(snapshot.title));
    if (class_name.find(L"#32770") != std::wstring::npos || class_name.find(L"dialog") != std::wstring::npos) {
        return true;
    }
    for (const std::wstring& token : {
             std::wstring(L"dialog"),
             std::wstring(L"properties"),
             std::wstring(L"options"),
             std::wstring(L"warning"),
             std::wstring(L"error"),
             std::wstring(L"confirm"),
             std::wstring(L"permission")}) {
        if (title.find(token) != std::wstring::npos) {
            return true;
        }
    }
    return false;
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

struct RelatedWindowScore {
    double score = 0.0;
    std::vector<std::string> reasons;
};

const WindowSnapshot* find_snapshot_by_hwnd(const std::vector<WindowSnapshot>& rows, long long hwnd) {
    if (hwnd <= 0) {
        return nullptr;
    }
    for (const WindowSnapshot& row : rows) {
        if (row.hwnd == hwnd) {
            return &row;
        }
    }
    return nullptr;
}

bool snapshot_is_descendant_of(
    const std::vector<WindowSnapshot>& rows,
    const WindowSnapshot& snapshot,
    long long ancestor_hwnd,
    int* relative_depth
) {
    if (relative_depth != nullptr) {
        *relative_depth = 0;
    }
    if (ancestor_hwnd <= 0 || snapshot.hwnd <= 0 || snapshot.hwnd == ancestor_hwnd) {
        return false;
    }
    long long current_owner_hwnd = snapshot.owner_hwnd;
    int depth = 0;
    int guard = 0;
    while (current_owner_hwnd > 0 && guard < 32) {
        ++depth;
        if (current_owner_hwnd == ancestor_hwnd) {
            if (relative_depth != nullptr) {
                *relative_depth = depth;
            }
            return true;
        }
        const WindowSnapshot* owner_row = find_snapshot_by_hwnd(rows, current_owner_hwnd);
        if (owner_row == nullptr) {
            break;
        }
        if (owner_row->owner_hwnd == current_owner_hwnd) {
            break;
        }
        current_owner_hwnd = owner_row->owner_hwnd;
        ++guard;
    }
    return false;
}

bool snapshot_matches_chain_query(
    const WindowSnapshot& snapshot,
    const std::wstring& query,
    const std::wstring& hint_query,
    const std::wstring& window_title
) {
    if (substring_match_score(utf8_to_wide(snapshot.title), query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.process_name), query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.process_name), hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), window_title) > 0.0) {
        return true;
    }
    return false;
}

std::vector<std::string> descendant_chain_titles(
    const std::vector<WindowSnapshot>& rows,
    const WindowSnapshot& candidate,
    const WindowSnapshot& descendant
) {
    std::vector<std::string> titles;
    if (candidate.hwnd <= 0 || descendant.hwnd <= 0 || candidate.hwnd == descendant.hwnd) {
        return titles;
    }

    const WindowSnapshot* current = &descendant;
    int guard = 0;
    while (current != nullptr && guard < 32) {
        if (!current->title.empty()) {
            titles.push_back(current->title);
        }
        if (current->hwnd == candidate.hwnd || current->owner_hwnd == candidate.hwnd) {
            break;
        }
        current = find_snapshot_by_hwnd(rows, current->owner_hwnd);
        ++guard;
    }
    std::reverse(titles.begin(), titles.end());
    titles.erase(
        std::unique(titles.begin(), titles.end(), [](const std::string& left, const std::string& right) {
            return left == right;
        }),
        titles.end()
    );
    return titles;
}

std::string child_chain_signature(
    long long candidate_hwnd,
    int direct_child_window_count,
    int descendant_chain_depth,
    const std::vector<std::string>& titles
) {
    std::ostringstream output;
    output << candidate_hwnd << "|" << direct_child_window_count << "|" << descendant_chain_depth;
    for (std::size_t index = 0; index < titles.size() && index < 5; ++index) {
        if (titles[index].empty()) {
            continue;
        }
        output << "|" << titles[index];
    }
    return output.str();
}

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

RelatedWindowScore score_related_window(
    const WindowSnapshot& snapshot,
    const std::wstring& query,
    const std::wstring& hint_query,
    const std::wstring& window_title,
    long long anchor_hwnd,
    long anchor_pid,
    long long anchor_root_owner_hwnd,
    int anchor_owner_chain_depth
) {
    RelatedWindowScore relation;
    const long long candidate_hwnd = snapshot.hwnd;
    const long long candidate_owner_hwnd = snapshot.owner_hwnd;
    const long long candidate_root_owner_hwnd = snapshot.root_owner_hwnd > 0 ? snapshot.root_owner_hwnd : snapshot.hwnd;
    const int candidate_owner_chain_depth = snapshot.owner_chain_depth;
    const double title_score = substring_match_score(utf8_to_wide(snapshot.title), window_title);
    const double query_score = std::max(
        substring_match_score(utf8_to_wide(snapshot.title), query),
        substring_match_score(utf8_to_wide(snapshot.process_name), query)
    );
    const double hint_query_score = std::max(
        substring_match_score(utf8_to_wide(snapshot.title), hint_query),
        substring_match_score(utf8_to_wide(snapshot.process_name), hint_query)
    );

    if (anchor_hwnd > 0 && candidate_hwnd == anchor_hwnd) {
        relation.score += 2.4;
        relation.reasons.push_back("exact_hwnd");
    }
    if (anchor_pid > 0 && snapshot.pid == anchor_pid) {
        relation.score += 1.15;
        relation.reasons.push_back("same_pid");
    }
    if (anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 1.05;
        relation.reasons.push_back("owned_by_hwnd");
    }
    if (anchor_root_owner_hwnd > 0 && candidate_root_owner_hwnd == anchor_root_owner_hwnd) {
        relation.score += 0.72;
        relation.reasons.push_back("same_root_owner");
    }
    if (title_score > 0.0) {
        relation.score += 0.95 * title_score;
        relation.reasons.push_back("window_title");
    }
    if (query_score > 0.0) {
        relation.score += 0.52 * query_score;
        relation.reasons.push_back("query");
    }
    if (hint_query_score > 0.0) {
        relation.score += 0.38 * hint_query_score;
        relation.reasons.push_back("hint_query");
    }
    if (query_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 1.2;
        relation.reasons.push_back("query_owned_child");
    } else if (query_score >= 0.95 && anchor_root_owner_hwnd > 0 && candidate_root_owner_hwnd == anchor_root_owner_hwnd) {
        relation.score += 0.7;
        relation.reasons.push_back("query_same_root_owner");
    }
    if (hint_query_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 0.85;
        relation.reasons.push_back("hint_query_owned_child");
    } else if (
        hint_query_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.48;
        relation.reasons.push_back("hint_query_same_root_owner");
    }
    if (anchor_root_owner_hwnd > 0 &&
        candidate_root_owner_hwnd == anchor_root_owner_hwnd &&
        candidate_owner_chain_depth > anchor_owner_chain_depth) {
        relation.score += std::min(0.24, 0.08 * std::max(1, candidate_owner_chain_depth - anchor_owner_chain_depth));
        relation.reasons.push_back("deeper_owner_chain");
    }
    if (snapshot.is_foreground) {
        relation.score += 0.08;
        relation.reasons.push_back("foreground");
    }
    if (snapshot.visible && snapshot.enabled) {
        relation.score += 0.05;
    }
    if (snapshot_is_dialog_like(snapshot) && (query_score > 0.0 || title_score > 0.0 || anchor_pid > 0)) {
        relation.score += 0.06;
        relation.reasons.push_back("dialog_related");
    }
    return relation;
}

std::string related_window_payload_json(
    const std::vector<WindowSnapshot>& rows,
    const WindowSnapshot& candidate,
    const std::vector<WindowSnapshot>& candidates,
    long long anchor_root_owner_hwnd,
    long anchor_pid,
    double top_score
) {
    std::vector<WindowSnapshot> same_root_owner_windows;
    std::vector<WindowSnapshot> related_windows;
    std::vector<WindowSnapshot> owner_chain_rows;
    int same_process_window_count = 0;
    int owner_link_count = 0;
    int same_root_owner_dialog_like_count = 0;
    int max_owner_chain_depth = candidate.owner_chain_depth;

    for (const WindowSnapshot& row : rows) {
        if (anchor_pid > 0 && row.pid == anchor_pid) {
            ++same_process_window_count;
        }
        if (anchor_root_owner_hwnd > 0 && row.root_owner_hwnd == anchor_root_owner_hwnd) {
            same_root_owner_windows.push_back(row);
            if (snapshot_is_dialog_like(row)) {
                ++same_root_owner_dialog_like_count;
            }
            if (row.owner_chain_depth > max_owner_chain_depth) {
                max_owner_chain_depth = row.owner_chain_depth;
            }
        }
        if (candidate.hwnd > 0 &&
            (row.owner_hwnd == candidate.hwnd ||
             row.owner_hwnd == candidate.owner_hwnd ||
             (candidate.root_owner_hwnd > 0 && row.root_owner_hwnd == candidate.root_owner_hwnd))) {
            related_windows.push_back(row);
        }
    }

    owner_link_count = static_cast<int>(std::count_if(
        related_windows.begin(),
        related_windows.end(),
        [](const WindowSnapshot& row) { return row.hwnd > 0; }
    ));

    for (const WindowSnapshot& row : rows) {
        if (row.hwnd == candidate.hwnd || row.hwnd == candidate.owner_hwnd || row.hwnd == candidate.root_owner_hwnd) {
            owner_chain_rows.push_back(row);
        }
    }
    std::sort(owner_chain_rows.begin(), owner_chain_rows.end(), [](const WindowSnapshot& left, const WindowSnapshot& right) {
        if (left.owner_chain_depth != right.owner_chain_depth) {
            return left.owner_chain_depth < right.owner_chain_depth;
        }
        return left.hwnd < right.hwnd;
    });

    std::ostringstream output;
    output << "{"
           << "\"status\":\"success\","
           << "\"backend\":\"cpp_cython\","
           << "\"candidate\":" << snapshot_to_json(candidate) << ","
           << "\"same_process_window_count\":" << same_process_window_count << ","
           << "\"related_window_count\":" << related_windows.size() << ","
           << "\"owner_link_count\":" << owner_link_count << ","
           << "\"owner_chain_visible\":" << ((candidate.owner_hwnd > 0 || owner_link_count > 0) ? "true" : "false") << ","
           << "\"same_root_owner_window_count\":" << same_root_owner_windows.size() << ","
           << "\"same_root_owner_dialog_like_count\":" << same_root_owner_dialog_like_count << ","
           << "\"candidate_root_owner_hwnd\":" << candidate.root_owner_hwnd << ","
           << "\"candidate_owner_chain_depth\":" << candidate.owner_chain_depth << ","
           << "\"max_owner_chain_depth\":" << max_owner_chain_depth << ","
           << "\"child_dialog_like_visible\":"
           << (std::any_of(
                   related_windows.begin(),
                   related_windows.end(),
                   [&candidate](const WindowSnapshot& row) {
                       return row.hwnd != candidate.hwnd && snapshot_is_dialog_like(row);
                   }) ? "true" : "false")
           << ","
           << "\"match_score\":" << top_score << ","
           << "\"same_root_owner_titles\":[";
    for (std::size_t index = 0; index < same_root_owner_windows.size() && index < 6; ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(same_root_owner_windows[index].title) << "\"";
    }
    output << "],\"same_root_owner_dialog_titles\":[";
    int dialog_title_index = 0;
    for (const WindowSnapshot& row : same_root_owner_windows) {
        if (!snapshot_is_dialog_like(row)) {
            continue;
        }
        if (dialog_title_index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(row.title) << "\"";
        ++dialog_title_index;
        if (dialog_title_index >= 6) {
            break;
        }
    }
    output << "],\"owner_chain_titles\":[";
    for (std::size_t index = 0; index < owner_chain_rows.size() && index < 8; ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(owner_chain_rows[index].title) << "\"";
    }
    output << "],\"candidates\":[";
    for (std::size_t index = 0; index < candidates.size() && index < 8; ++index) {
        if (index > 0) {
            output << ",";
        }
        output << snapshot_to_json(candidates[index]);
    }
    output << "]}";
    return output.str();
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

std::string reacquire_related_window_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& window_title_utf8,
    long long hwnd_value,
    long pid_value,
    int limit
) {
    const int safe_limit = std::max(1, std::min(limit, 500));
    const HWND foreground = GetForegroundWindow();

    std::vector<WindowSnapshot> rows;
    rows.reserve(static_cast<std::size_t>(safe_limit));
    EnumWindowsContext context{&rows, foreground, safe_limit};
    EnumWindows(EnumWindowsListProc, reinterpret_cast<LPARAM>(&context));

    WindowSnapshot anchor;
    bool anchor_found = false;
    if (hwnd_value > 0) {
        for (const WindowSnapshot& row : rows) {
            if (row.hwnd == hwnd_value) {
                anchor = row;
                anchor_found = true;
                break;
            }
        }
    }
    if (!anchor_found && foreground != nullptr) {
        collect_window_snapshot(foreground, foreground, anchor);
        anchor_found = anchor.hwnd > 0;
    }

    const std::wstring query = utf8_to_wide(query_utf8);
    const std::wstring hint_query = utf8_to_wide(hint_query_utf8);
    const std::wstring window_title = utf8_to_wide(window_title_utf8);
    const long long anchor_hwnd = anchor_found ? anchor.hwnd : hwnd_value;
    const long anchor_pid = pid_value > 0 ? pid_value : (anchor_found ? anchor.pid : 0);
    const long long anchor_root_owner_hwnd =
        anchor_found && anchor.root_owner_hwnd > 0 ? anchor.root_owner_hwnd : (anchor_hwnd > 0 ? anchor_hwnd : 0);
    const int anchor_owner_chain_depth = anchor_found ? anchor.owner_chain_depth : 0;

    std::vector<std::pair<double, WindowSnapshot>> scored_rows;
    for (const WindowSnapshot& row : rows) {
        const RelatedWindowScore relation = score_related_window(
            row,
            query,
            hint_query,
            window_title,
            anchor_hwnd,
            anchor_pid,
            anchor_root_owner_hwnd,
            anchor_owner_chain_depth
        );
        if (relation.score <= 0.0) {
            continue;
        }
        scored_rows.push_back({relation.score, row});
    }

    std::sort(scored_rows.begin(), scored_rows.end(), [](const auto& left, const auto& right) {
        if (left.first != right.first) {
            return left.first > right.first;
        }
        if (left.second.is_foreground != right.second.is_foreground) {
            return left.second.is_foreground;
        }
        const int left_area = std::max(0, left.second.right - left.second.left) * std::max(0, left.second.bottom - left.second.top);
        const int right_area = std::max(0, right.second.right - right.second.left) * std::max(0, right.second.bottom - right.second.top);
        if (left_area != right_area) {
            return left_area > right_area;
        }
        return left.second.title < right.second.title;
    });

    if (scored_rows.empty()) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    std::vector<WindowSnapshot> candidates;
    candidates.reserve(std::min<std::size_t>(8, scored_rows.size()));
    for (std::size_t index = 0; index < scored_rows.size() && index < 8; ++index) {
        candidates.push_back(scored_rows[index].second);
    }
    const WindowSnapshot& candidate = scored_rows.front().second;
    const double top_score = std::round(scored_rows.front().first * 1000.0) / 1000.0;
    if (top_score < 0.42) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    return related_window_payload_json(
        rows,
        candidate,
        candidates,
        candidate.root_owner_hwnd > 0 ? candidate.root_owner_hwnd : anchor_root_owner_hwnd,
        candidate.pid > 0 ? candidate.pid : anchor_pid,
        top_score
    );
}

std::string trace_related_window_chain_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& window_title_utf8,
    long long hwnd_value,
    long pid_value,
    int limit
) {
    const int safe_limit = std::max(1, std::min(limit, 500));
    const HWND foreground = GetForegroundWindow();

    std::vector<WindowSnapshot> rows;
    rows.reserve(static_cast<std::size_t>(safe_limit));
    EnumWindowsContext context{&rows, foreground, safe_limit};
    EnumWindows(EnumWindowsListProc, reinterpret_cast<LPARAM>(&context));

    WindowSnapshot anchor;
    bool anchor_found = false;
    if (hwnd_value > 0) {
        for (const WindowSnapshot& row : rows) {
            if (row.hwnd == hwnd_value) {
                anchor = row;
                anchor_found = true;
                break;
            }
        }
    }
    if (!anchor_found && foreground != nullptr) {
        collect_window_snapshot(foreground, foreground, anchor);
        anchor_found = anchor.hwnd > 0;
    }

    const std::wstring query = utf8_to_wide(query_utf8);
    const std::wstring hint_query = utf8_to_wide(hint_query_utf8);
    const std::wstring window_title = utf8_to_wide(window_title_utf8);
    const long long anchor_hwnd = anchor_found ? anchor.hwnd : hwnd_value;
    const long anchor_pid = pid_value > 0 ? pid_value : (anchor_found ? anchor.pid : 0);
    const long long anchor_root_owner_hwnd =
        anchor_found && anchor.root_owner_hwnd > 0 ? anchor.root_owner_hwnd : (anchor_hwnd > 0 ? anchor_hwnd : 0);
    const int anchor_owner_chain_depth = anchor_found ? anchor.owner_chain_depth : 0;

    std::vector<std::pair<double, WindowSnapshot>> scored_rows;
    for (const WindowSnapshot& row : rows) {
        const RelatedWindowScore relation = score_related_window(
            row,
            query,
            hint_query,
            window_title,
            anchor_hwnd,
            anchor_pid,
            anchor_root_owner_hwnd,
            anchor_owner_chain_depth
        );
        if (relation.score <= 0.0) {
            continue;
        }
        scored_rows.push_back({relation.score, row});
    }

    std::sort(scored_rows.begin(), scored_rows.end(), [](const auto& left, const auto& right) {
        if (left.first != right.first) {
            return left.first > right.first;
        }
        if (left.second.is_foreground != right.second.is_foreground) {
            return left.second.is_foreground;
        }
        const int left_area = std::max(0, left.second.right - left.second.left) * std::max(0, left.second.bottom - left.second.top);
        const int right_area = std::max(0, right.second.right - right.second.left) * std::max(0, right.second.bottom - right.second.top);
        if (left_area != right_area) {
            return left_area > right_area;
        }
        return left.second.title < right.second.title;
    });

    if (scored_rows.empty()) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    const WindowSnapshot& candidate = scored_rows.front().second;
    const double top_score = std::round(scored_rows.front().first * 1000.0) / 1000.0;
    if (top_score < 0.42) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    std::vector<WindowSnapshot> direct_children;
    std::vector<WindowSnapshot> descendants;
    std::vector<std::pair<int, WindowSnapshot>> descendant_depth_rows;
    int direct_child_dialog_like_count = 0;
    int descendant_chain_depth = 0;
    int descendant_dialog_chain_depth = 0;
    int descendant_query_match_count = 0;

    for (const WindowSnapshot& row : rows) {
        if (row.hwnd == candidate.hwnd) {
            continue;
        }
        if (candidate.hwnd > 0 && row.owner_hwnd == candidate.hwnd) {
            direct_children.push_back(row);
            if (snapshot_is_dialog_like(row)) {
                ++direct_child_dialog_like_count;
            }
        }
        int relative_depth = 0;
        if (!snapshot_is_descendant_of(rows, row, candidate.hwnd, &relative_depth)) {
            continue;
        }
        descendants.push_back(row);
        descendant_depth_rows.push_back({relative_depth, row});
        descendant_chain_depth = std::max(descendant_chain_depth, relative_depth);
        if (snapshot_is_dialog_like(row)) {
            descendant_dialog_chain_depth = std::max(descendant_dialog_chain_depth, relative_depth);
        }
        if (snapshot_matches_chain_query(row, query, hint_query, window_title)) {
            ++descendant_query_match_count;
        }
    }

    std::sort(direct_children.begin(), direct_children.end(), [](const WindowSnapshot& left, const WindowSnapshot& right) {
        if (left.owner_chain_depth != right.owner_chain_depth) {
            return left.owner_chain_depth < right.owner_chain_depth;
        }
        return left.title < right.title;
    });
    std::sort(descendant_depth_rows.begin(), descendant_depth_rows.end(), [&](const auto& left, const auto& right) {
        const bool left_query_match = snapshot_matches_chain_query(left.second, query, hint_query, window_title);
        const bool right_query_match = snapshot_matches_chain_query(right.second, query, hint_query, window_title);
        if (left_query_match != right_query_match) {
            return left_query_match;
        }
        if (left.first != right.first) {
            return left.first > right.first;
        }
        if (snapshot_is_dialog_like(left.second) != snapshot_is_dialog_like(right.second)) {
            return snapshot_is_dialog_like(left.second);
        }
        return left.second.title < right.second.title;
    });

    std::vector<std::string> direct_child_titles;
    for (const WindowSnapshot& row : direct_children) {
        if (!row.title.empty()) {
            direct_child_titles.push_back(row.title);
        }
        if (direct_child_titles.size() >= 8) {
            break;
        }
    }

    WindowSnapshot preferred_descendant;
    bool preferred_descendant_found = false;
    std::vector<std::string> descendant_titles;
    if (!descendant_depth_rows.empty()) {
        preferred_descendant = descendant_depth_rows.front().second;
        preferred_descendant_found = preferred_descendant.hwnd > 0;
        descendant_titles = descendant_chain_titles(rows, candidate, preferred_descendant);
    }

    const std::string chain_signature = child_chain_signature(
        candidate.hwnd,
        static_cast<int>(direct_children.size()),
        descendant_chain_depth,
        descendant_titles
    );

    std::ostringstream output;
    output << "{"
           << "\"status\":\"success\","
           << "\"backend\":\"cpp_cython\","
           << "\"candidate\":" << snapshot_to_json(candidate) << ","
           << "\"match_score\":" << top_score << ","
           << "\"direct_child_window_count\":" << direct_children.size() << ","
           << "\"direct_child_dialog_like_count\":" << direct_child_dialog_like_count << ","
           << "\"descendant_chain_depth\":" << descendant_chain_depth << ","
           << "\"descendant_dialog_chain_depth\":" << descendant_dialog_chain_depth << ","
           << "\"descendant_query_match_count\":" << descendant_query_match_count << ","
           << "\"child_chain_signature\":\"" << json_escape(chain_signature) << "\","
           << "\"direct_child_titles\":[";
    for (std::size_t index = 0; index < direct_child_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(direct_child_titles[index]) << "\"";
    }
    output << "],\"descendant_chain_titles\":[";
    for (std::size_t index = 0; index < descendant_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(descendant_titles[index]) << "\"";
    }
    output << "],\"preferred_descendant\":";
    if (preferred_descendant_found) {
        output << snapshot_to_json(preferred_descendant);
    } else {
        output << "null";
    }
    output << "}";
    return output.str();
}

}  // namespace jarvis::native
