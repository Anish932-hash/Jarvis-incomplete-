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

bool collect_window_snapshot(HWND hwnd, HWND foreground, WindowSnapshot& snapshot);

bool focus_snapshot_target(
    const WindowSnapshot& target_snapshot,
    WindowSnapshot& refreshed_snapshot,
    bool& focus_applied
) {
    if (target_snapshot.hwnd <= 0) {
        focus_applied = false;
        return false;
    }
    const HWND target = reinterpret_cast<HWND>(static_cast<intptr_t>(target_snapshot.hwnd));
    if (target == nullptr || !IsWindow(target)) {
        focus_applied = false;
        return false;
    }
    if (IsIconic(target)) {
        ShowWindow(target, SW_RESTORE);
    } else {
        ShowWindow(target, SW_SHOW);
    }
    BringWindowToTop(target);
    const BOOL focus_result = SetForegroundWindow(target);
    const HWND foreground = GetForegroundWindow();
    focus_applied = (focus_result != FALSE || foreground == target);
    return collect_window_snapshot(target, foreground, refreshed_snapshot);
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

std::wstring normalize_whitespace(const std::wstring& value) {
    std::wstringstream input(value);
    std::wstring token;
    std::wstring output;
    while (input >> token) {
        if (!output.empty()) {
            output.append(L" ");
        }
        output.append(token);
    }
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
    const std::wstring& descendant_hint_query,
    const std::wstring& campaign_hint_query,
    const std::wstring& campaign_preferred_title,
    const std::wstring& portfolio_hint_query,
    const std::wstring& portfolio_preferred_title,
    const std::wstring& confirmation_hint_query,
    const std::wstring& confirmation_preferred_title,
    const std::wstring& preferred_title,
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
    if (substring_match_score(utf8_to_wide(snapshot.title), descendant_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.process_name), descendant_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), campaign_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.process_name), campaign_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), campaign_preferred_title) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), portfolio_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.process_name), portfolio_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), portfolio_preferred_title) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), confirmation_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.process_name), confirmation_hint_query) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), confirmation_preferred_title) > 0.0) {
        return true;
    }
    if (substring_match_score(utf8_to_wide(snapshot.title), preferred_title) > 0.0) {
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

std::vector<std::wstring> parse_title_sequence_utf8(const std::string& sequence_utf8) {
    std::vector<std::wstring> sequence;
    if (sequence_utf8.empty()) {
        return sequence;
    }
    std::size_t start = 0;
    while (start < sequence_utf8.size()) {
        const std::size_t delimiter = sequence_utf8.find("||", start);
        const std::string token = sequence_utf8.substr(
            start,
            delimiter == std::string::npos ? std::string::npos : delimiter - start
        );
        const std::wstring wide_token = utf8_to_wide(token);
        const std::wstring normalized = normalize_whitespace(wide_token);
        if (!normalized.empty()
            && std::none_of(
                sequence.begin(),
                sequence.end(),
                [&normalized](const std::wstring& existing) {
                    return to_lower_copy(existing) == to_lower_copy(normalized);
                })) {
            sequence.push_back(normalized);
            if (sequence.size() >= 8) {
                break;
            }
        }
        if (delimiter == std::string::npos) {
            break;
        }
        start = delimiter + 2;
    }
    return sequence;
}

std::wstring expected_descendant_sequence_title(
    const std::vector<std::wstring>& sequence,
    const WindowSnapshot& candidate
) {
    if (sequence.empty()) {
        return L"";
    }
    const std::wstring candidate_title = normalize_whitespace(utf8_to_wide(candidate.title));
    if (candidate_title.empty()) {
        return sequence.front();
    }
    for (std::size_t index = 0; index < sequence.size(); ++index) {
        if (substring_match_score(candidate_title, sequence[index]) >= 0.72
            || substring_match_score(sequence[index], candidate_title) >= 0.72) {
            if (index + 1 < sequence.size()) {
                return sequence[index + 1];
            }
            return L"";
        }
    }
    return sequence.front();
}

int sequence_match_count(
    const std::vector<std::pair<int, WindowSnapshot>>& descendant_rows,
    const std::vector<std::wstring>& sequence
) {
    if (descendant_rows.empty() || sequence.empty()) {
        return 0;
    }
    int count = 0;
    for (const auto& entry : descendant_rows) {
        const std::wstring row_title = normalize_whitespace(utf8_to_wide(entry.second.title));
        if (row_title.empty()) {
            continue;
        }
        const bool matched = std::any_of(
            sequence.begin(),
            sequence.end(),
            [&row_title](const std::wstring& title) {
                return substring_match_score(row_title, title) >= 0.72
                    || substring_match_score(title, row_title) >= 0.72;
            });
        if (matched) {
            ++count;
        }
    }
    return count;
}

double preferred_sequence_match_score(
    const std::wstring& title,
    const std::vector<std::wstring>& sequence,
    const std::wstring& expected_title
) {
    double score = 0.0;
    if (!expected_title.empty()) {
        score = std::max(
            score,
            std::max(
                substring_match_score(title, expected_title),
                substring_match_score(expected_title, title)
            )
        );
    }
    for (const std::wstring& entry : sequence) {
        score = std::max(
            score,
            std::max(
                substring_match_score(title, entry),
                substring_match_score(entry, title)
            )
        );
    }
    return score;
}

struct DescendantChainMetrics {
    std::vector<WindowSnapshot> direct_children;
    std::vector<std::pair<int, WindowSnapshot>> descendant_depth_rows;
    std::vector<std::string> direct_child_titles;
    std::vector<std::string> descendant_titles;
    WindowSnapshot preferred_descendant;
    bool preferred_descendant_found = false;
    int direct_child_dialog_like_count = 0;
    int descendant_chain_depth = 0;
    int descendant_dialog_chain_depth = 0;
    int descendant_query_match_count = 0;
    int descendant_hint_title_match_count = 0;
    int campaign_descendant_hint_title_match_count = 0;
    int portfolio_descendant_hint_title_match_count = 0;
    int confirmation_descendant_hint_title_match_count = 0;
    int descendant_sequence_match_count = 0;
    int campaign_descendant_sequence_match_count = 0;
    int portfolio_descendant_sequence_match_count = 0;
    int confirmation_descendant_sequence_match_count = 0;
    double preferred_descendant_match_score = 0.0;
    double preferred_descendant_sequence_match_score = 0.0;
    double preferred_campaign_descendant_sequence_match_score = 0.0;
    double preferred_portfolio_descendant_sequence_match_score = 0.0;
    double preferred_confirmation_descendant_sequence_match_score = 0.0;
    double confirmation_sequence_progress_score = 0.0;
    double confirmation_chain_readiness = 0.0;
    double descendant_focus_strength = 0.0;
    std::string expected_descendant_sequence_title;
    std::string expected_campaign_descendant_sequence_title;
    std::string expected_portfolio_descendant_sequence_title;
    std::string expected_confirmation_descendant_sequence_title;
};

DescendantChainMetrics analyze_descendant_chain(
    const std::vector<WindowSnapshot>& rows,
    const WindowSnapshot& candidate,
    const std::wstring& query,
    const std::wstring& hint_query,
    const std::wstring& descendant_hint_query,
    const std::vector<std::wstring>& descendant_title_sequence,
    const std::wstring& campaign_hint_query,
    const std::wstring& campaign_preferred_title,
    const std::vector<std::wstring>& campaign_descendant_title_sequence,
    const std::wstring& portfolio_hint_query,
    const std::wstring& portfolio_preferred_title,
    const std::vector<std::wstring>& portfolio_descendant_title_sequence,
    const std::wstring& confirmation_hint_query,
    const std::wstring& confirmation_preferred_title,
    const std::vector<std::wstring>& confirmation_title_sequence,
    const std::wstring& preferred_title,
    const std::wstring& window_title
) {
    DescendantChainMetrics metrics;
    if (candidate.hwnd <= 0) {
        return metrics;
    }

    for (const WindowSnapshot& row : rows) {
        if (row.hwnd == candidate.hwnd) {
            continue;
        }
        if (row.owner_hwnd == candidate.hwnd) {
            metrics.direct_children.push_back(row);
            if (snapshot_is_dialog_like(row)) {
                ++metrics.direct_child_dialog_like_count;
            }
        }
        int relative_depth = 0;
        if (!snapshot_is_descendant_of(rows, row, candidate.hwnd, &relative_depth)) {
            continue;
        }
        metrics.descendant_depth_rows.push_back({relative_depth, row});
        metrics.descendant_chain_depth = std::max(metrics.descendant_chain_depth, relative_depth);
        if (snapshot_is_dialog_like(row)) {
            metrics.descendant_dialog_chain_depth =
                std::max(metrics.descendant_dialog_chain_depth, relative_depth);
        }
        if (snapshot_matches_chain_query(
                row,
                query,
                hint_query,
                descendant_hint_query,
                campaign_hint_query,
                campaign_preferred_title,
                portfolio_hint_query,
                portfolio_preferred_title,
                confirmation_hint_query,
                confirmation_preferred_title,
                preferred_title,
                window_title
            )) {
            ++metrics.descendant_query_match_count;
        }
        const std::wstring row_title = utf8_to_wide(row.title);
        if (!descendant_hint_query.empty()
            && substring_match_score(row_title, descendant_hint_query) > 0.0) {
            ++metrics.descendant_hint_title_match_count;
        }
        if (!campaign_hint_query.empty()
            && substring_match_score(row_title, campaign_hint_query) > 0.0) {
            ++metrics.campaign_descendant_hint_title_match_count;
        }
        if (!portfolio_hint_query.empty()
            && substring_match_score(row_title, portfolio_hint_query) > 0.0) {
            ++metrics.portfolio_descendant_hint_title_match_count;
        }
        if (!confirmation_hint_query.empty()
            && substring_match_score(row_title, confirmation_hint_query) > 0.0) {
            ++metrics.confirmation_descendant_hint_title_match_count;
        }
    }

    std::sort(metrics.direct_children.begin(), metrics.direct_children.end(), [](const WindowSnapshot& left, const WindowSnapshot& right) {
        if (left.owner_chain_depth != right.owner_chain_depth) {
            return left.owner_chain_depth < right.owner_chain_depth;
        }
        return left.title < right.title;
    });
    const std::wstring expected_sequence_title =
        expected_descendant_sequence_title(descendant_title_sequence, candidate);
    const std::wstring expected_campaign_sequence_title =
        expected_descendant_sequence_title(campaign_descendant_title_sequence, candidate);
    const std::wstring expected_portfolio_sequence_title =
        expected_descendant_sequence_title(portfolio_descendant_title_sequence, candidate);
    const std::wstring expected_confirmation_sequence_title =
        expected_descendant_sequence_title(confirmation_title_sequence, candidate);
    metrics.expected_descendant_sequence_title = wide_to_utf8(expected_sequence_title);
    metrics.expected_campaign_descendant_sequence_title = wide_to_utf8(expected_campaign_sequence_title);
    metrics.expected_portfolio_descendant_sequence_title = wide_to_utf8(expected_portfolio_sequence_title);
    metrics.expected_confirmation_descendant_sequence_title = wide_to_utf8(expected_confirmation_sequence_title);

    std::sort(metrics.descendant_depth_rows.begin(), metrics.descendant_depth_rows.end(), [&](const auto& left, const auto& right) {
        const bool left_query_match = snapshot_matches_chain_query(
            left.second,
            query,
            hint_query,
            descendant_hint_query,
            campaign_hint_query,
            campaign_preferred_title,
            portfolio_hint_query,
            portfolio_preferred_title,
            confirmation_hint_query,
            confirmation_preferred_title,
            preferred_title,
            window_title
        );
        const bool right_query_match = snapshot_matches_chain_query(
            right.second,
            query,
            hint_query,
            descendant_hint_query,
            campaign_hint_query,
            campaign_preferred_title,
            portfolio_hint_query,
            portfolio_preferred_title,
            confirmation_hint_query,
            confirmation_preferred_title,
            preferred_title,
            window_title
        );
        if (left_query_match != right_query_match) {
            return left_query_match;
        }
        const std::wstring left_title = utf8_to_wide(left.second.title);
        const std::wstring right_title = utf8_to_wide(right.second.title);
        const double left_expected_sequence = std::max(
            std::max(
                substring_match_score(left_title, expected_sequence_title),
                substring_match_score(expected_sequence_title, left_title)
            ),
            std::max(
                std::max(
                    substring_match_score(left_title, expected_campaign_sequence_title),
                    substring_match_score(expected_campaign_sequence_title, left_title)
                ),
                std::max(
                    std::max(
                        substring_match_score(left_title, expected_portfolio_sequence_title),
                        substring_match_score(expected_portfolio_sequence_title, left_title)
                    ),
                    std::max(
                        substring_match_score(left_title, expected_confirmation_sequence_title),
                        substring_match_score(expected_confirmation_sequence_title, left_title)
                    )
                )
            )
        );
        const double right_expected_sequence = std::max(
            std::max(
                substring_match_score(right_title, expected_sequence_title),
                substring_match_score(expected_sequence_title, right_title)
            ),
            std::max(
                std::max(
                    substring_match_score(right_title, expected_campaign_sequence_title),
                    substring_match_score(expected_campaign_sequence_title, right_title)
                ),
                std::max(
                    std::max(
                        substring_match_score(right_title, expected_portfolio_sequence_title),
                        substring_match_score(expected_portfolio_sequence_title, right_title)
                    ),
                    std::max(
                        substring_match_score(right_title, expected_confirmation_sequence_title),
                        substring_match_score(expected_confirmation_sequence_title, right_title)
                    )
                )
            )
        );
        if (left_expected_sequence != right_expected_sequence) {
            return left_expected_sequence > right_expected_sequence;
        }
        const double left_preferred = std::max(
            std::max(
                substring_match_score(left_title, preferred_title),
                substring_match_score(left_title, campaign_preferred_title)
            ),
            std::max(
                substring_match_score(left_title, portfolio_preferred_title),
                substring_match_score(left_title, confirmation_preferred_title)
            )
        );
        const double right_preferred = std::max(
            std::max(
                substring_match_score(right_title, preferred_title),
                substring_match_score(right_title, campaign_preferred_title)
            ),
            std::max(
                substring_match_score(right_title, portfolio_preferred_title),
                substring_match_score(right_title, confirmation_preferred_title)
            )
        );
        if (left_preferred != right_preferred) {
            return left_preferred > right_preferred;
        }
        if (left.first != right.first) {
            return left.first > right.first;
        }
        if (snapshot_is_dialog_like(left.second) != snapshot_is_dialog_like(right.second)) {
            return snapshot_is_dialog_like(left.second);
        }
        return left.second.title < right.second.title;
    });

    for (const WindowSnapshot& row : metrics.direct_children) {
        if (!row.title.empty()) {
            metrics.direct_child_titles.push_back(row.title);
        }
        if (metrics.direct_child_titles.size() >= 8) {
            break;
        }
    }

    if (!metrics.descendant_depth_rows.empty()) {
        metrics.preferred_descendant = metrics.descendant_depth_rows.front().second;
        metrics.preferred_descendant_found = metrics.preferred_descendant.hwnd > 0;
        metrics.descendant_titles =
            descendant_chain_titles(rows, candidate, metrics.preferred_descendant);
        metrics.descendant_sequence_match_count = sequence_match_count(
            metrics.descendant_depth_rows,
            descendant_title_sequence
        );
        metrics.campaign_descendant_sequence_match_count = sequence_match_count(
            metrics.descendant_depth_rows,
            campaign_descendant_title_sequence
        );
        metrics.portfolio_descendant_sequence_match_count = sequence_match_count(
            metrics.descendant_depth_rows,
            portfolio_descendant_title_sequence
        );
        metrics.confirmation_descendant_sequence_match_count = sequence_match_count(
            metrics.descendant_depth_rows,
            confirmation_title_sequence
        );

        const std::wstring preferred_descendant_title =
            utf8_to_wide(metrics.preferred_descendant.title);
        const double descendant_hint_score = std::max(
            substring_match_score(preferred_descendant_title, descendant_hint_query),
            substring_match_score(
                utf8_to_wide(metrics.preferred_descendant.process_name),
                descendant_hint_query
            )
        );
        const double campaign_hint_score = std::max(
            substring_match_score(preferred_descendant_title, campaign_hint_query),
            substring_match_score(
                utf8_to_wide(metrics.preferred_descendant.process_name),
                campaign_hint_query
            )
        );
        const double portfolio_hint_score = std::max(
            substring_match_score(preferred_descendant_title, portfolio_hint_query),
            substring_match_score(
                utf8_to_wide(metrics.preferred_descendant.process_name),
                portfolio_hint_query
            )
        );
        const double confirmation_hint_score = std::max(
            substring_match_score(preferred_descendant_title, confirmation_hint_query),
            substring_match_score(
                utf8_to_wide(metrics.preferred_descendant.process_name),
                confirmation_hint_query
            )
        );
        const double preferred_title_score = substring_match_score(
            preferred_descendant_title,
            preferred_title
        );
        const double campaign_preferred_title_score = substring_match_score(
            preferred_descendant_title,
            campaign_preferred_title
        );
        const double portfolio_preferred_title_score = substring_match_score(
            preferred_descendant_title,
            portfolio_preferred_title
        );
        const double confirmation_preferred_title_score = substring_match_score(
            preferred_descendant_title,
            confirmation_preferred_title
        );
        metrics.preferred_descendant_match_score = std::max(
            std::max(
                std::max(
                    std::max(descendant_hint_score, campaign_hint_score),
                    std::max(portfolio_hint_score, confirmation_hint_score)
                ),
                std::max(preferred_title_score, confirmation_preferred_title_score)
            ),
            std::max(campaign_preferred_title_score, portfolio_preferred_title_score)
        );
        metrics.preferred_descendant_sequence_match_score = preferred_sequence_match_score(
            preferred_descendant_title,
            descendant_title_sequence,
            expected_sequence_title
        );
        metrics.preferred_campaign_descendant_sequence_match_score = preferred_sequence_match_score(
            preferred_descendant_title,
            campaign_descendant_title_sequence,
            expected_campaign_sequence_title
        );
        metrics.preferred_portfolio_descendant_sequence_match_score = preferred_sequence_match_score(
            preferred_descendant_title,
            portfolio_descendant_title_sequence,
            expected_portfolio_sequence_title
        );
        metrics.preferred_confirmation_descendant_sequence_match_score = preferred_sequence_match_score(
            preferred_descendant_title,
            confirmation_title_sequence,
            expected_confirmation_sequence_title
        );

        double confirmation_progress = 0.0;
        if (!expected_confirmation_sequence_title.empty()) {
            confirmation_progress += 0.08;
        }
        confirmation_progress += std::min(
            0.46,
            metrics.preferred_confirmation_descendant_sequence_match_score * 0.46
        );
        confirmation_progress += std::min(
            0.18,
            0.05 * metrics.confirmation_descendant_sequence_match_count
        );
        confirmation_progress += std::min(
            0.12,
            0.04 * metrics.confirmation_descendant_hint_title_match_count
        );
        confirmation_progress += std::min(
            0.08,
            0.03 * metrics.descendant_dialog_chain_depth
        );
        if (snapshot_is_dialog_like(metrics.preferred_descendant)) {
            confirmation_progress += 0.06;
        }
        metrics.confirmation_sequence_progress_score =
            std::clamp(confirmation_progress, 0.0, 1.0);

        double confirmation_readiness = 0.2;
        confirmation_readiness += std::min(
            0.36,
            metrics.confirmation_sequence_progress_score * 0.36
        );
        confirmation_readiness += std::min(
            0.14,
            metrics.descendant_focus_strength * 0.14
        );
        confirmation_readiness += std::min(
            0.1,
            0.04 * metrics.direct_child_dialog_like_count
        );
        confirmation_readiness += std::min(
            0.1,
            0.035 * metrics.descendant_dialog_chain_depth
        );
        if (metrics.preferred_descendant.owner_hwnd == candidate.hwnd) {
            confirmation_readiness += 0.05;
        }
        if (!metrics.expected_confirmation_descendant_sequence_title.empty()) {
            confirmation_readiness += 0.05;
        }
        metrics.confirmation_chain_readiness =
            std::clamp(confirmation_readiness, 0.0, 1.0);

        double focus_strength = 0.38;
        focus_strength += std::min(0.16, 0.04 * metrics.descendant_chain_depth);
        focus_strength += std::min(0.14, 0.04 * metrics.descendant_dialog_chain_depth);
        focus_strength += std::min(0.18, 0.05 * metrics.descendant_query_match_count);
        focus_strength += std::min(0.12, 0.08 * metrics.descendant_hint_title_match_count);
        focus_strength += std::min(0.1, 0.08 * metrics.campaign_descendant_hint_title_match_count);
        focus_strength += std::min(0.1, 0.08 * metrics.portfolio_descendant_hint_title_match_count);
        focus_strength += std::min(0.1, 0.08 * metrics.confirmation_descendant_hint_title_match_count);
        focus_strength += std::min(0.14, metrics.preferred_descendant_match_score * 0.14);
        focus_strength += std::min(0.08, 0.03 * metrics.descendant_sequence_match_count);
        focus_strength += std::min(0.07, 0.025 * metrics.campaign_descendant_sequence_match_count);
        focus_strength += std::min(0.07, 0.025 * metrics.portfolio_descendant_sequence_match_count);
        focus_strength += std::min(0.08, 0.03 * metrics.confirmation_descendant_sequence_match_count);
        focus_strength += std::min(0.12, metrics.preferred_descendant_sequence_match_score * 0.12);
        focus_strength += std::min(0.1, metrics.preferred_campaign_descendant_sequence_match_score * 0.1);
        focus_strength += std::min(0.1, metrics.preferred_portfolio_descendant_sequence_match_score * 0.1);
        focus_strength += std::min(0.12, metrics.preferred_confirmation_descendant_sequence_match_score * 0.12);
        focus_strength += std::min(0.08, metrics.confirmation_sequence_progress_score * 0.08);
        focus_strength += std::min(0.08, metrics.confirmation_chain_readiness * 0.08);
        if (snapshot_is_dialog_like(metrics.preferred_descendant)) {
            focus_strength += 0.06;
        }
        if (metrics.preferred_descendant.owner_hwnd == candidate.hwnd) {
            focus_strength += 0.05;
        }
        metrics.descendant_focus_strength = std::clamp(focus_strength, 0.0, 1.0);
        metrics.confirmation_chain_readiness = std::clamp(
            metrics.confirmation_chain_readiness
                + std::min(0.14, metrics.descendant_focus_strength * 0.14),
            0.0,
            1.0
        );
    }

    return metrics;
}

std::vector<WindowSnapshot> enumerate_window_snapshots(int limit) {
    const HWND foreground = GetForegroundWindow();
    std::vector<WindowSnapshot> rows;
    rows.reserve(static_cast<std::size_t>(std::max(1, std::min(limit, 500))));
    EnumWindowsContext context{&rows, foreground, std::max(1, std::min(limit, 500))};
    EnumWindows(EnumWindowsListProc, reinterpret_cast<LPARAM>(&context));
    return rows;
}

double descendant_chain_follow_quality(const DescendantChainMetrics& metrics) {
    if (!metrics.preferred_descendant_found) {
        return 0.0;
    }
    double quality = 0.22;
    quality += std::min(0.26, metrics.descendant_focus_strength * 0.3);
    quality += std::min(0.18, metrics.preferred_descendant_match_score * 0.18);
    quality += std::min(0.12, 0.04 * metrics.descendant_chain_depth);
    quality += std::min(0.1, 0.04 * metrics.descendant_dialog_chain_depth);
    quality += std::min(0.12, 0.03 * metrics.descendant_query_match_count);
    quality += std::min(0.08, 0.03 * metrics.descendant_hint_title_match_count);
    quality += std::min(0.06, 0.025 * metrics.campaign_descendant_hint_title_match_count);
    quality += std::min(0.06, 0.025 * metrics.portfolio_descendant_hint_title_match_count);
    quality += std::min(0.07, 0.03 * metrics.confirmation_descendant_hint_title_match_count);
    quality += std::min(0.08, 0.025 * metrics.descendant_sequence_match_count);
    quality += std::min(0.06, 0.02 * metrics.campaign_descendant_sequence_match_count);
    quality += std::min(0.06, 0.02 * metrics.portfolio_descendant_sequence_match_count);
    quality += std::min(0.08, 0.025 * metrics.confirmation_descendant_sequence_match_count);
    quality += std::min(0.12, metrics.preferred_descendant_sequence_match_score * 0.12);
    quality += std::min(0.1, metrics.preferred_campaign_descendant_sequence_match_score * 0.1);
    quality += std::min(0.1, metrics.preferred_portfolio_descendant_sequence_match_score * 0.1);
    quality += std::min(0.12, metrics.preferred_confirmation_descendant_sequence_match_score * 0.12);
    quality += std::min(0.12, metrics.confirmation_sequence_progress_score * 0.12);
    quality += std::min(0.16, metrics.confirmation_chain_readiness * 0.16);
    if (snapshot_is_dialog_like(metrics.preferred_descendant)) {
        quality += 0.05;
    }
    if (metrics.preferred_descendant.owner_hwnd > 0) {
        quality += 0.04;
    }
    return std::clamp(quality, 0.0, 1.0);
}

bool descendant_chain_follow_allowed(const DescendantChainMetrics& metrics) {
    if (!metrics.preferred_descendant_found) {
        return false;
    }
    const double quality = descendant_chain_follow_quality(metrics);
    return quality >= 0.56
        && (
            metrics.descendant_query_match_count > 0
            || metrics.preferred_descendant_match_score >= 0.72
            || metrics.preferred_descendant_sequence_match_score >= 0.72
            || metrics.preferred_campaign_descendant_sequence_match_score >= 0.72
            || metrics.preferred_portfolio_descendant_sequence_match_score >= 0.72
            || metrics.preferred_confirmation_descendant_sequence_match_score >= 0.72
            || metrics.confirmation_chain_readiness >= 0.72
            || metrics.confirmation_descendant_sequence_match_count > 0
            || metrics.descendant_dialog_chain_depth > 0
            || metrics.direct_child_dialog_like_count > 0
        );
}

double snapshot_bidirectional_match_score(
    const WindowSnapshot& snapshot,
    const std::wstring& needle
) {
    if (needle.empty()) {
        return 0.0;
    }
    const std::wstring title = utf8_to_wide(snapshot.title);
    const std::wstring process_name = utf8_to_wide(snapshot.process_name);
    return std::max(
        std::max(
            substring_match_score(title, needle),
            substring_match_score(needle, title)
        ),
        std::max(
            substring_match_score(process_name, needle),
            substring_match_score(needle, process_name)
        )
    );
}

double best_snapshot_sequence_match_score(
    const WindowSnapshot& snapshot,
    const std::vector<std::wstring>& sequence
) {
    double score = 0.0;
    for (const std::wstring& entry : sequence) {
        score = std::max(score, snapshot_bidirectional_match_score(snapshot, entry));
    }
    return score;
}

struct ChainAnchorRecoveryCandidate {
    WindowSnapshot snapshot;
    bool found = false;
    double score = 0.0;
    double match_score = 0.0;
    std::string reason;
    std::string matched_title;
};

ChainAnchorRecoveryCandidate recover_descendant_chain_anchor(
    const std::vector<WindowSnapshot>& rows,
    const WindowSnapshot& missing_anchor,
    const std::wstring& query,
    const std::wstring& hint_query,
    const std::wstring& descendant_hint_query,
    const std::vector<std::wstring>& descendant_title_sequence,
    const std::wstring& campaign_hint_query,
    const std::wstring& campaign_preferred_title,
    const std::vector<std::wstring>& campaign_descendant_title_sequence,
    const std::wstring& portfolio_hint_query,
    const std::wstring& portfolio_preferred_title,
    const std::vector<std::wstring>& portfolio_descendant_title_sequence,
    const std::wstring& confirmation_hint_query,
    const std::wstring& confirmation_preferred_title,
    const std::vector<std::wstring>& confirmation_title_sequence,
    const std::wstring& preferred_title,
    const std::wstring& window_title,
    const DescendantChainMetrics& prior_metrics,
    const std::vector<long long>& visited_descendant_hwnds
) {
    ChainAnchorRecoveryCandidate best;
    const long long root_owner_hwnd =
        missing_anchor.root_owner_hwnd > 0 ? missing_anchor.root_owner_hwnd : missing_anchor.hwnd;
    if (root_owner_hwnd <= 0) {
        return best;
    }
    const std::wstring anchor_title = normalize_whitespace(utf8_to_wide(missing_anchor.title));
    const std::wstring expected_sequence_title =
        normalize_whitespace(utf8_to_wide(prior_metrics.expected_descendant_sequence_title));
    const std::wstring expected_campaign_sequence_title =
        normalize_whitespace(utf8_to_wide(prior_metrics.expected_campaign_descendant_sequence_title));
    const std::wstring expected_portfolio_sequence_title =
        normalize_whitespace(utf8_to_wide(prior_metrics.expected_portfolio_descendant_sequence_title));
    const std::wstring expected_confirmation_sequence_title =
        normalize_whitespace(utf8_to_wide(prior_metrics.expected_confirmation_descendant_sequence_title));

    for (const WindowSnapshot& row : rows) {
        if (row.hwnd <= 0 || row.hwnd == missing_anchor.hwnd || row.hwnd == root_owner_hwnd) {
            continue;
        }
        if (row.root_owner_hwnd != root_owner_hwnd) {
            continue;
        }
        if (std::find(visited_descendant_hwnds.begin(), visited_descendant_hwnds.end(), row.hwnd)
            != visited_descendant_hwnds.end()) {
            continue;
        }

        const double expected_sequence_score =
            snapshot_bidirectional_match_score(row, expected_sequence_title);
        const double expected_campaign_sequence_score =
            snapshot_bidirectional_match_score(row, expected_campaign_sequence_title);
        const double expected_portfolio_sequence_score =
            snapshot_bidirectional_match_score(row, expected_portfolio_sequence_title);
        const double expected_confirmation_sequence_score =
            snapshot_bidirectional_match_score(row, expected_confirmation_sequence_title);
        const double descendant_sequence_score =
            best_snapshot_sequence_match_score(row, descendant_title_sequence);
        const double campaign_descendant_sequence_score =
            best_snapshot_sequence_match_score(row, campaign_descendant_title_sequence);
        const double portfolio_descendant_sequence_score =
            best_snapshot_sequence_match_score(row, portfolio_descendant_title_sequence);
        const double confirmation_descendant_sequence_score =
            best_snapshot_sequence_match_score(row, confirmation_title_sequence);
        const double query_score = std::max(
            snapshot_bidirectional_match_score(row, query),
            snapshot_bidirectional_match_score(row, hint_query)
        );
        const double descendant_hint_score =
            snapshot_bidirectional_match_score(row, descendant_hint_query);
        const double campaign_hint_score =
            snapshot_bidirectional_match_score(row, campaign_hint_query);
        const double portfolio_hint_score =
            snapshot_bidirectional_match_score(row, portfolio_hint_query);
        const double confirmation_hint_score =
            snapshot_bidirectional_match_score(row, confirmation_hint_query);
        const double preferred_title_score =
            snapshot_bidirectional_match_score(row, preferred_title);
        const double campaign_preferred_title_score =
            snapshot_bidirectional_match_score(row, campaign_preferred_title);
        const double portfolio_preferred_title_score =
            snapshot_bidirectional_match_score(row, portfolio_preferred_title);
        const double confirmation_preferred_title_score =
            snapshot_bidirectional_match_score(row, confirmation_preferred_title);
        const double anchor_title_score =
            snapshot_bidirectional_match_score(row, anchor_title);
        const double window_title_score =
            snapshot_bidirectional_match_score(row, window_title);

        double score = 0.38;
        score += std::min(0.16, 0.04 * std::max(1, row.owner_chain_depth));
        if (row.pid > 0 && row.pid == missing_anchor.pid) {
            score += 0.18;
        }
        if (snapshot_is_dialog_like(row)) {
            score += 0.08;
        }
        if (row.is_foreground) {
            score += 0.06;
        }
        score += std::min(
            0.28,
            std::max(
                std::max(expected_sequence_score, expected_campaign_sequence_score),
                std::max(expected_portfolio_sequence_score, expected_confirmation_sequence_score)
            ) * 0.28
        );
        score += std::min(
            0.18,
            std::max(
                std::max(descendant_sequence_score, campaign_descendant_sequence_score),
                std::max(portfolio_descendant_sequence_score, confirmation_descendant_sequence_score)
            ) * 0.18
        );
        score += std::min(
            0.16,
            std::max(
                std::max(
                    std::max(query_score, descendant_hint_score),
                    campaign_hint_score
                ),
                std::max(portfolio_hint_score, confirmation_hint_score)
            ) * 0.16
        );
        score += std::min(
            0.14,
            std::max(
                std::max(preferred_title_score, campaign_preferred_title_score),
                std::max(portfolio_preferred_title_score, confirmation_preferred_title_score)
            ) * 0.14
        );
        score += std::min(0.1, anchor_title_score * 0.1);
        score += std::min(0.08, window_title_score * 0.08);

        const double match_score = std::max(
            std::max(
                std::max(
                    std::max(expected_sequence_score, expected_campaign_sequence_score),
                    std::max(expected_portfolio_sequence_score, expected_confirmation_sequence_score)
                ),
                std::max(
                    std::max(descendant_sequence_score, campaign_descendant_sequence_score),
                    std::max(portfolio_descendant_sequence_score, confirmation_descendant_sequence_score)
                )
            ),
            std::max(
                std::max(
                    std::max(query_score, descendant_hint_score),
                    std::max(campaign_hint_score, std::max(portfolio_hint_score, confirmation_hint_score))
                ),
                std::max(
                    std::max(
                        std::max(preferred_title_score, campaign_preferred_title_score),
                        std::max(portfolio_preferred_title_score, confirmation_preferred_title_score)
                    ),
                    std::max(anchor_title_score, window_title_score)
                )
            )
        );

        if (score <= 0.0 || match_score <= 0.0) {
            continue;
        }

        std::string recovery_reason = "same_root_owner_family";
        std::string matched_title;
        if (expected_sequence_score >= expected_campaign_sequence_score
            && expected_sequence_score >= expected_portfolio_sequence_score
            && expected_sequence_score >= expected_confirmation_sequence_score
            && expected_sequence_score >= descendant_sequence_score
            && expected_sequence_score >= campaign_descendant_sequence_score
            && expected_sequence_score >= portfolio_descendant_sequence_score
            && expected_sequence_score >= confirmation_descendant_sequence_score
            && expected_sequence_score >= query_score
            && expected_sequence_score >= descendant_hint_score
            && expected_sequence_score >= campaign_hint_score
            && expected_sequence_score >= portfolio_hint_score
            && expected_sequence_score >= confirmation_hint_score
            && expected_sequence_score >= preferred_title_score
            && expected_sequence_score >= campaign_preferred_title_score
            && expected_sequence_score >= portfolio_preferred_title_score
            && expected_sequence_score >= confirmation_preferred_title_score
            && expected_sequence_score >= anchor_title_score
            && expected_sequence_score >= window_title_score
            && !prior_metrics.expected_descendant_sequence_title.empty()) {
            recovery_reason = "expected_descendant_sequence_title";
            matched_title = prior_metrics.expected_descendant_sequence_title;
        } else if (expected_campaign_sequence_score >= expected_portfolio_sequence_score
            && expected_campaign_sequence_score >= expected_confirmation_sequence_score
            && expected_campaign_sequence_score >= descendant_sequence_score
            && expected_campaign_sequence_score >= campaign_descendant_sequence_score
            && expected_campaign_sequence_score >= portfolio_descendant_sequence_score
            && expected_campaign_sequence_score >= confirmation_descendant_sequence_score
            && expected_campaign_sequence_score >= query_score
            && expected_campaign_sequence_score >= descendant_hint_score
            && expected_campaign_sequence_score >= campaign_hint_score
            && expected_campaign_sequence_score >= portfolio_hint_score
            && expected_campaign_sequence_score >= confirmation_hint_score
            && expected_campaign_sequence_score >= preferred_title_score
            && expected_campaign_sequence_score >= campaign_preferred_title_score
            && expected_campaign_sequence_score >= portfolio_preferred_title_score
            && expected_campaign_sequence_score >= confirmation_preferred_title_score
            && expected_campaign_sequence_score >= anchor_title_score
            && expected_campaign_sequence_score >= window_title_score
            && !prior_metrics.expected_campaign_descendant_sequence_title.empty()) {
            recovery_reason = "expected_campaign_descendant_sequence_title";
            matched_title = prior_metrics.expected_campaign_descendant_sequence_title;
        } else if (expected_portfolio_sequence_score >= expected_confirmation_sequence_score
            && expected_portfolio_sequence_score >= descendant_sequence_score
            && expected_portfolio_sequence_score >= campaign_descendant_sequence_score
            && expected_portfolio_sequence_score >= portfolio_descendant_sequence_score
            && expected_portfolio_sequence_score >= confirmation_descendant_sequence_score
            && expected_portfolio_sequence_score >= query_score
            && expected_portfolio_sequence_score >= descendant_hint_score
            && expected_portfolio_sequence_score >= campaign_hint_score
            && expected_portfolio_sequence_score >= portfolio_hint_score
            && expected_portfolio_sequence_score >= confirmation_hint_score
            && expected_portfolio_sequence_score >= preferred_title_score
            && expected_portfolio_sequence_score >= campaign_preferred_title_score
            && expected_portfolio_sequence_score >= portfolio_preferred_title_score
            && expected_portfolio_sequence_score >= confirmation_preferred_title_score
            && expected_portfolio_sequence_score >= anchor_title_score
            && expected_portfolio_sequence_score >= window_title_score
            && !prior_metrics.expected_portfolio_descendant_sequence_title.empty()) {
            recovery_reason = "expected_portfolio_descendant_sequence_title";
            matched_title = prior_metrics.expected_portfolio_descendant_sequence_title;
        } else if (expected_confirmation_sequence_score >= descendant_sequence_score
            && expected_confirmation_sequence_score >= campaign_descendant_sequence_score
            && expected_confirmation_sequence_score >= portfolio_descendant_sequence_score
            && expected_confirmation_sequence_score >= confirmation_descendant_sequence_score
            && expected_confirmation_sequence_score >= query_score
            && expected_confirmation_sequence_score >= descendant_hint_score
            && expected_confirmation_sequence_score >= campaign_hint_score
            && expected_confirmation_sequence_score >= portfolio_hint_score
            && expected_confirmation_sequence_score >= confirmation_hint_score
            && expected_confirmation_sequence_score >= preferred_title_score
            && expected_confirmation_sequence_score >= campaign_preferred_title_score
            && expected_confirmation_sequence_score >= portfolio_preferred_title_score
            && expected_confirmation_sequence_score >= confirmation_preferred_title_score
            && expected_confirmation_sequence_score >= anchor_title_score
            && expected_confirmation_sequence_score >= window_title_score
            && !prior_metrics.expected_confirmation_descendant_sequence_title.empty()) {
            recovery_reason = "expected_confirmation_descendant_sequence_title";
            matched_title = prior_metrics.expected_confirmation_descendant_sequence_title;
        } else if (descendant_sequence_score >= campaign_descendant_sequence_score
            && descendant_sequence_score >= portfolio_descendant_sequence_score
            && descendant_sequence_score >= confirmation_descendant_sequence_score
            && descendant_sequence_score >= query_score
            && descendant_sequence_score >= descendant_hint_score
            && descendant_sequence_score >= campaign_hint_score
            && descendant_sequence_score >= portfolio_hint_score
            && descendant_sequence_score >= confirmation_hint_score
            && descendant_sequence_score >= preferred_title_score
            && descendant_sequence_score >= campaign_preferred_title_score
            && descendant_sequence_score >= portfolio_preferred_title_score
            && descendant_sequence_score >= confirmation_preferred_title_score
            && descendant_sequence_score >= anchor_title_score
            && descendant_sequence_score >= window_title_score) {
            recovery_reason = "descendant_title_sequence";
        } else if (campaign_descendant_sequence_score >= portfolio_descendant_sequence_score
            && campaign_descendant_sequence_score >= confirmation_descendant_sequence_score
            && campaign_descendant_sequence_score >= query_score
            && campaign_descendant_sequence_score >= descendant_hint_score
            && campaign_descendant_sequence_score >= campaign_hint_score
            && campaign_descendant_sequence_score >= portfolio_hint_score
            && campaign_descendant_sequence_score >= confirmation_hint_score
            && campaign_descendant_sequence_score >= preferred_title_score
            && campaign_descendant_sequence_score >= campaign_preferred_title_score
            && campaign_descendant_sequence_score >= portfolio_preferred_title_score
            && campaign_descendant_sequence_score >= confirmation_preferred_title_score
            && campaign_descendant_sequence_score >= anchor_title_score
            && campaign_descendant_sequence_score >= window_title_score) {
            recovery_reason = "campaign_descendant_title_sequence";
        } else if (portfolio_descendant_sequence_score >= confirmation_descendant_sequence_score
            && portfolio_descendant_sequence_score >= query_score
            && portfolio_descendant_sequence_score >= descendant_hint_score
            && portfolio_descendant_sequence_score >= campaign_hint_score
            && portfolio_descendant_sequence_score >= portfolio_hint_score
            && portfolio_descendant_sequence_score >= confirmation_hint_score
            && portfolio_descendant_sequence_score >= preferred_title_score
            && portfolio_descendant_sequence_score >= campaign_preferred_title_score
            && portfolio_descendant_sequence_score >= portfolio_preferred_title_score
            && portfolio_descendant_sequence_score >= confirmation_preferred_title_score
            && portfolio_descendant_sequence_score >= anchor_title_score
            && portfolio_descendant_sequence_score >= window_title_score) {
            recovery_reason = "portfolio_descendant_title_sequence";
        } else if (confirmation_descendant_sequence_score >= query_score
            && confirmation_descendant_sequence_score >= descendant_hint_score
            && confirmation_descendant_sequence_score >= campaign_hint_score
            && confirmation_descendant_sequence_score >= portfolio_hint_score
            && confirmation_descendant_sequence_score >= confirmation_hint_score
            && confirmation_descendant_sequence_score >= preferred_title_score
            && confirmation_descendant_sequence_score >= campaign_preferred_title_score
            && confirmation_descendant_sequence_score >= portfolio_preferred_title_score
            && confirmation_descendant_sequence_score >= confirmation_preferred_title_score
            && confirmation_descendant_sequence_score >= anchor_title_score
            && confirmation_descendant_sequence_score >= window_title_score) {
            recovery_reason = "confirmation_descendant_title_sequence";
        } else if (descendant_hint_score >= query_score
            && descendant_hint_score >= campaign_hint_score
            && descendant_hint_score >= portfolio_hint_score
            && descendant_hint_score >= confirmation_hint_score
            && descendant_hint_score >= preferred_title_score
            && descendant_hint_score >= campaign_preferred_title_score
            && descendant_hint_score >= portfolio_preferred_title_score
            && descendant_hint_score >= confirmation_preferred_title_score
            && descendant_hint_score >= anchor_title_score
            && descendant_hint_score >= window_title_score) {
            recovery_reason = "descendant_hint_query";
            matched_title = wide_to_utf8(descendant_hint_query);
        } else if (campaign_hint_score >= query_score
            && campaign_hint_score >= portfolio_hint_score
            && campaign_hint_score >= confirmation_hint_score
            && campaign_hint_score >= preferred_title_score
            && campaign_hint_score >= campaign_preferred_title_score
            && campaign_hint_score >= portfolio_preferred_title_score
            && campaign_hint_score >= confirmation_preferred_title_score
            && campaign_hint_score >= anchor_title_score
            && campaign_hint_score >= window_title_score) {
            recovery_reason = "campaign_hint_query";
            matched_title = wide_to_utf8(campaign_hint_query);
        } else if (portfolio_hint_score >= confirmation_hint_score
            && portfolio_hint_score >= query_score
            && portfolio_hint_score >= preferred_title_score
            && portfolio_hint_score >= campaign_preferred_title_score
            && portfolio_hint_score >= portfolio_preferred_title_score
            && portfolio_hint_score >= confirmation_preferred_title_score
            && portfolio_hint_score >= anchor_title_score
            && portfolio_hint_score >= window_title_score) {
            recovery_reason = "portfolio_hint_query";
            matched_title = wide_to_utf8(portfolio_hint_query);
        } else if (confirmation_hint_score >= query_score
            && confirmation_hint_score >= preferred_title_score
            && confirmation_hint_score >= campaign_preferred_title_score
            && confirmation_hint_score >= portfolio_preferred_title_score
            && confirmation_hint_score >= confirmation_preferred_title_score
            && confirmation_hint_score >= anchor_title_score
            && confirmation_hint_score >= window_title_score) {
            recovery_reason = "confirmation_hint_query";
            matched_title = wide_to_utf8(confirmation_hint_query);
        } else if (preferred_title_score >= campaign_preferred_title_score
            && preferred_title_score >= portfolio_preferred_title_score
            && preferred_title_score >= confirmation_preferred_title_score
            && preferred_title_score >= anchor_title_score
            && preferred_title_score >= window_title_score) {
            recovery_reason = "preferred_title";
            matched_title = wide_to_utf8(preferred_title);
        } else if (campaign_preferred_title_score >= portfolio_preferred_title_score
            && campaign_preferred_title_score >= confirmation_preferred_title_score
            && campaign_preferred_title_score >= anchor_title_score
            && campaign_preferred_title_score >= window_title_score) {
            recovery_reason = "campaign_preferred_title";
            matched_title = wide_to_utf8(campaign_preferred_title);
        } else if (portfolio_preferred_title_score >= confirmation_preferred_title_score
            && portfolio_preferred_title_score >= anchor_title_score
            && portfolio_preferred_title_score >= window_title_score) {
            recovery_reason = "portfolio_preferred_title";
            matched_title = wide_to_utf8(portfolio_preferred_title);
        } else if (confirmation_preferred_title_score >= anchor_title_score
            && confirmation_preferred_title_score >= window_title_score) {
            recovery_reason = "confirmation_preferred_title";
            matched_title = wide_to_utf8(confirmation_preferred_title);
        } else if (anchor_title_score >= window_title_score) {
            recovery_reason = "anchor_title";
            matched_title = wide_to_utf8(anchor_title);
        } else if (window_title_score > 0.0) {
            recovery_reason = "window_title";
            matched_title = wide_to_utf8(window_title);
        }

        if (score > best.score) {
            best.snapshot = row;
            best.score = score;
            best.match_score = match_score;
            best.reason = recovery_reason;
            best.matched_title = matched_title;
        }
    }

    best.found = best.snapshot.hwnd > 0
        && (
            (best.score >= 0.86 && best.match_score >= 0.58)
            || best.match_score >= 0.82
        );
    return best;
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
    const std::wstring& descendant_hint_query,
    const std::wstring& campaign_hint_query,
    const std::wstring& campaign_preferred_title,
    const std::wstring& portfolio_hint_query,
    const std::wstring& portfolio_preferred_title,
    const std::wstring& confirmation_hint_query,
    const std::wstring& confirmation_preferred_title,
    const std::wstring& preferred_title,
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
    const double descendant_hint_query_score = std::max(
        substring_match_score(utf8_to_wide(snapshot.title), descendant_hint_query),
        substring_match_score(utf8_to_wide(snapshot.process_name), descendant_hint_query)
    );
    const double campaign_hint_query_score = std::max(
        substring_match_score(utf8_to_wide(snapshot.title), campaign_hint_query),
        substring_match_score(utf8_to_wide(snapshot.process_name), campaign_hint_query)
    );
    const double campaign_preferred_title_score = substring_match_score(
        utf8_to_wide(snapshot.title),
        campaign_preferred_title
    );
    const double portfolio_hint_query_score = std::max(
        substring_match_score(utf8_to_wide(snapshot.title), portfolio_hint_query),
        substring_match_score(utf8_to_wide(snapshot.process_name), portfolio_hint_query)
    );
    const double portfolio_preferred_title_score = substring_match_score(
        utf8_to_wide(snapshot.title),
        portfolio_preferred_title
    );
    const double confirmation_hint_query_score = std::max(
        substring_match_score(utf8_to_wide(snapshot.title), confirmation_hint_query),
        substring_match_score(utf8_to_wide(snapshot.process_name), confirmation_hint_query)
    );
    const double confirmation_preferred_title_score = substring_match_score(
        utf8_to_wide(snapshot.title),
        confirmation_preferred_title
    );
    const double preferred_title_score = substring_match_score(
        utf8_to_wide(snapshot.title),
        preferred_title
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
    if (descendant_hint_query_score > 0.0) {
        relation.score += 0.36 * descendant_hint_query_score;
        relation.reasons.push_back("descendant_hint_query");
    }
    if (campaign_hint_query_score > 0.0) {
        relation.score += 0.34 * campaign_hint_query_score;
        relation.reasons.push_back("campaign_hint_query");
    }
    if (campaign_preferred_title_score > 0.0) {
        relation.score += 0.42 * campaign_preferred_title_score;
        relation.reasons.push_back("campaign_preferred_title");
    }
    if (portfolio_hint_query_score > 0.0) {
        relation.score += 0.34 * portfolio_hint_query_score;
        relation.reasons.push_back("portfolio_hint_query");
    }
    if (portfolio_preferred_title_score > 0.0) {
        relation.score += 0.42 * portfolio_preferred_title_score;
        relation.reasons.push_back("portfolio_preferred_title");
    }
    if (confirmation_hint_query_score > 0.0) {
        relation.score += 0.36 * confirmation_hint_query_score;
        relation.reasons.push_back("confirmation_hint_query");
    }
    if (confirmation_preferred_title_score > 0.0) {
        relation.score += 0.46 * confirmation_preferred_title_score;
        relation.reasons.push_back("confirmation_preferred_title");
    }
    if (preferred_title_score > 0.0) {
        relation.score += 0.44 * preferred_title_score;
        relation.reasons.push_back("preferred_title");
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
    if (descendant_hint_query_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 0.9;
        relation.reasons.push_back("descendant_hint_owned_child");
    } else if (
        descendant_hint_query_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.52;
        relation.reasons.push_back("descendant_hint_same_root_owner");
    }
    if (campaign_hint_query_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 0.82;
        relation.reasons.push_back("campaign_hint_owned_child");
    } else if (
        campaign_hint_query_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.5;
        relation.reasons.push_back("campaign_hint_same_root_owner");
    }
    if (campaign_preferred_title_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 0.96;
        relation.reasons.push_back("campaign_preferred_title_owned_child");
    } else if (
        campaign_preferred_title_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.62;
        relation.reasons.push_back("campaign_preferred_title_same_root_owner");
    }
    if (portfolio_hint_query_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 0.82;
        relation.reasons.push_back("portfolio_hint_owned_child");
    } else if (
        portfolio_hint_query_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.5;
        relation.reasons.push_back("portfolio_hint_same_root_owner");
    }
    if (portfolio_preferred_title_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 0.96;
        relation.reasons.push_back("portfolio_preferred_title_owned_child");
    } else if (
        portfolio_preferred_title_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.62;
        relation.reasons.push_back("portfolio_preferred_title_same_root_owner");
    }
    if (confirmation_hint_query_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 0.88;
        relation.reasons.push_back("confirmation_hint_owned_child");
    } else if (
        confirmation_hint_query_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.56;
        relation.reasons.push_back("confirmation_hint_same_root_owner");
    }
    if (confirmation_preferred_title_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 1.02;
        relation.reasons.push_back("confirmation_preferred_title_owned_child");
    } else if (
        confirmation_preferred_title_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.66;
        relation.reasons.push_back("confirmation_preferred_title_same_root_owner");
    }
    if (preferred_title_score >= 0.95 && anchor_hwnd > 0 && candidate_owner_hwnd == anchor_hwnd) {
        relation.score += 1.1;
        relation.reasons.push_back("preferred_title_owned_child");
    } else if (
        preferred_title_score >= 0.95
        && anchor_root_owner_hwnd > 0
        && candidate_root_owner_hwnd == anchor_root_owner_hwnd
    ) {
        relation.score += 0.66;
        relation.reasons.push_back("preferred_title_same_root_owner");
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
    const DescendantChainMetrics& descendant_metrics,
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
           << "\"descendant_chain_depth\":" << descendant_metrics.descendant_chain_depth << ","
           << "\"descendant_dialog_chain_depth\":" << descendant_metrics.descendant_dialog_chain_depth << ","
           << "\"descendant_query_match_count\":" << descendant_metrics.descendant_query_match_count << ","
           << "\"descendant_hint_title_match_count\":" << descendant_metrics.descendant_hint_title_match_count << ","
           << "\"campaign_descendant_hint_title_match_count\":"
           << descendant_metrics.campaign_descendant_hint_title_match_count << ","
           << "\"portfolio_descendant_hint_title_match_count\":"
           << descendant_metrics.portfolio_descendant_hint_title_match_count << ","
           << "\"confirmation_descendant_hint_title_match_count\":"
           << descendant_metrics.confirmation_descendant_hint_title_match_count << ","
           << "\"descendant_sequence_match_count\":"
           << descendant_metrics.descendant_sequence_match_count << ","
           << "\"campaign_descendant_sequence_match_count\":"
           << descendant_metrics.campaign_descendant_sequence_match_count << ","
           << "\"portfolio_descendant_sequence_match_count\":"
           << descendant_metrics.portfolio_descendant_sequence_match_count << ","
           << "\"confirmation_descendant_sequence_match_count\":"
           << descendant_metrics.confirmation_descendant_sequence_match_count << ","
           << "\"expected_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_descendant_sequence_title) << "\","
           << "\"expected_campaign_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_campaign_descendant_sequence_title) << "\","
           << "\"expected_portfolio_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_portfolio_descendant_sequence_title) << "\","
           << "\"expected_confirmation_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_confirmation_descendant_sequence_title) << "\","
           << "\"preferred_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_descendant_sequence_match_score << ","
           << "\"preferred_campaign_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_campaign_descendant_sequence_match_score << ","
           << "\"preferred_portfolio_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_portfolio_descendant_sequence_match_score << ","
           << "\"preferred_confirmation_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_confirmation_descendant_sequence_match_score << ","
           << "\"confirmation_sequence_progress_score\":"
           << descendant_metrics.confirmation_sequence_progress_score << ","
           << "\"confirmation_chain_readiness\":"
           << descendant_metrics.confirmation_chain_readiness << ","
           << "\"preferred_descendant_match_score\":" << descendant_metrics.preferred_descendant_match_score << ","
           << "\"descendant_focus_strength\":" << descendant_metrics.descendant_focus_strength << ","
           << "\"direct_child_window_count\":" << descendant_metrics.direct_children.size() << ","
           << "\"direct_child_dialog_like_count\":" << descendant_metrics.direct_child_dialog_like_count << ","
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
    output << "],\"direct_child_titles\":[";
    for (std::size_t index = 0; index < descendant_metrics.direct_child_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(descendant_metrics.direct_child_titles[index]) << "\"";
    }
    output << "],\"descendant_chain_titles\":[";
    for (std::size_t index = 0; index < descendant_metrics.descendant_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(descendant_metrics.descendant_titles[index]) << "\"";
    }
    output << "],\"preferred_descendant\":";
    if (descendant_metrics.preferred_descendant_found) {
        output << snapshot_to_json(descendant_metrics.preferred_descendant);
    } else {
        output << "null";
    }
    output << ",\"owner_chain_titles\":[";
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

    WindowSnapshot target_snapshot;
    target_snapshot.hwnd = static_cast<long long>(reinterpret_cast<intptr_t>(target));
    WindowSnapshot snapshot;
    bool focus_applied = false;
    if (!focus_snapshot_target(target_snapshot, snapshot, focus_applied)) {
        return build_error_json("Window was focused but could not be inspected.");
    }

    std::ostringstream output;
    output << "{\"status\":\"success\",\"backend\":\"cpp_cython\",\"focus_applied\":"
           << (focus_applied ? "true" : "false")
           << ",\"window\":" << snapshot_to_json(snapshot) << "}";
    return output.str();
}

std::string focus_related_window_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& descendant_hint_query_utf8,
    const std::string& descendant_title_sequence_utf8,
    const std::string& campaign_hint_query_utf8,
    const std::string& campaign_preferred_title_utf8,
    const std::string& campaign_descendant_title_sequence_utf8,
    const std::string& portfolio_hint_query_utf8,
    const std::string& portfolio_preferred_title_utf8,
    const std::string& portfolio_descendant_title_sequence_utf8,
    const std::string& confirmation_hint_query_utf8,
    const std::string& confirmation_preferred_title_utf8,
    const std::string& confirmation_title_sequence_utf8,
    const std::string& preferred_title_utf8,
    const std::string& window_title_utf8,
    long long hwnd_value,
    long pid_value,
    int follow_descendant_chain_value,
    int max_descendant_focus_steps,
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
    const std::wstring descendant_hint_query = utf8_to_wide(descendant_hint_query_utf8);
    const std::vector<std::wstring> descendant_title_sequence =
        parse_title_sequence_utf8(descendant_title_sequence_utf8);
    const std::wstring campaign_hint_query = utf8_to_wide(campaign_hint_query_utf8);
    const std::wstring campaign_preferred_title = utf8_to_wide(campaign_preferred_title_utf8);
    const std::vector<std::wstring> campaign_descendant_title_sequence =
        parse_title_sequence_utf8(campaign_descendant_title_sequence_utf8);
    const std::wstring portfolio_hint_query = utf8_to_wide(portfolio_hint_query_utf8);
    const std::wstring portfolio_preferred_title = utf8_to_wide(portfolio_preferred_title_utf8);
    const std::vector<std::wstring> portfolio_descendant_title_sequence =
        parse_title_sequence_utf8(portfolio_descendant_title_sequence_utf8);
    const std::wstring confirmation_hint_query = utf8_to_wide(confirmation_hint_query_utf8);
    const std::wstring confirmation_preferred_title =
        utf8_to_wide(confirmation_preferred_title_utf8);
    const std::vector<std::wstring> confirmation_title_sequence =
        parse_title_sequence_utf8(confirmation_title_sequence_utf8);
    const std::wstring preferred_title = utf8_to_wide(preferred_title_utf8);
    const std::wstring window_title = utf8_to_wide(window_title_utf8);
    const bool follow_descendant_chain_requested = follow_descendant_chain_value != 0;
    const int safe_max_descendant_focus_steps = std::max(1, std::min(max_descendant_focus_steps, 6));
    const long long anchor_hwnd = anchor_found ? anchor.hwnd : hwnd_value;
    const long anchor_pid = pid_value > 0 ? pid_value : (anchor_found ? anchor.pid : 0);
    const long long anchor_root_owner_hwnd =
        anchor_found && anchor.root_owner_hwnd > 0 ? anchor.root_owner_hwnd : (anchor_hwnd > 0 ? anchor_hwnd : 0);
    const int anchor_owner_chain_depth = anchor_found ? anchor.owner_chain_depth : 0;

    struct ScoredRelatedWindowCandidate {
        double score = 0.0;
        WindowSnapshot row;
        DescendantChainMetrics descendant_metrics;
    };
    std::vector<ScoredRelatedWindowCandidate> scored_rows;
    for (const WindowSnapshot& row : rows) {
        RelatedWindowScore relation = score_related_window(
            row,
            query,
            hint_query,
            descendant_hint_query,
            campaign_hint_query,
            campaign_preferred_title,
            portfolio_hint_query,
            portfolio_preferred_title,
            confirmation_hint_query,
            confirmation_preferred_title,
            preferred_title,
            window_title,
            anchor_hwnd,
            anchor_pid,
            anchor_root_owner_hwnd,
            anchor_owner_chain_depth
        );
        const DescendantChainMetrics descendant_metrics = analyze_descendant_chain(
            rows,
            row,
            query,
            hint_query,
            descendant_hint_query,
            descendant_title_sequence,
            campaign_hint_query,
            campaign_preferred_title,
            campaign_descendant_title_sequence,
            portfolio_hint_query,
            portfolio_preferred_title,
            portfolio_descendant_title_sequence,
            confirmation_hint_query,
            confirmation_preferred_title,
            confirmation_title_sequence,
            preferred_title,
            window_title
        );
        if (descendant_metrics.preferred_descendant_found) {
            relation.score += 0.18;
            relation.reasons.push_back("preferred_descendant_available");
        }
        if (descendant_metrics.descendant_query_match_count > 0) {
            relation.score += std::min(
                0.7,
                0.16 + (0.12 * descendant_metrics.descendant_query_match_count)
            );
            relation.reasons.push_back("descendant_query_match");
        }
        if (descendant_metrics.descendant_hint_title_match_count > 0) {
            relation.score += std::min(
                0.54,
                0.12 + (0.1 * descendant_metrics.descendant_hint_title_match_count)
            );
            relation.reasons.push_back("descendant_hint_title_match");
        }
        if (descendant_metrics.campaign_descendant_hint_title_match_count > 0) {
            relation.score += std::min(
                0.48,
                0.1 + (0.1 * descendant_metrics.campaign_descendant_hint_title_match_count)
            );
            relation.reasons.push_back("campaign_descendant_hint_title_match");
        }
        if (descendant_metrics.portfolio_descendant_hint_title_match_count > 0) {
            relation.score += std::min(
                0.48,
                0.1 + (0.1 * descendant_metrics.portfolio_descendant_hint_title_match_count)
            );
            relation.reasons.push_back("portfolio_descendant_hint_title_match");
        }
        if (descendant_metrics.descendant_sequence_match_count > 0) {
            relation.score += std::min(
                0.44,
                0.1 + (0.08 * descendant_metrics.descendant_sequence_match_count)
            );
            relation.reasons.push_back("descendant_sequence_match");
        }
        if (descendant_metrics.campaign_descendant_sequence_match_count > 0) {
            relation.score += std::min(
                0.38,
                0.08 + (0.07 * descendant_metrics.campaign_descendant_sequence_match_count)
            );
            relation.reasons.push_back("campaign_descendant_sequence_match");
        }
        if (descendant_metrics.portfolio_descendant_sequence_match_count > 0) {
            relation.score += std::min(
                0.38,
                0.08 + (0.07 * descendant_metrics.portfolio_descendant_sequence_match_count)
            );
            relation.reasons.push_back("portfolio_descendant_sequence_match");
        }
        if (descendant_metrics.preferred_descendant_match_score > 0.0) {
            relation.score += std::min(
                0.46,
                0.12 + (descendant_metrics.preferred_descendant_match_score * 0.28)
            );
            relation.reasons.push_back("preferred_descendant_match");
        }
        if (descendant_metrics.preferred_descendant_sequence_match_score > 0.0) {
            relation.score += std::min(
                0.42,
                0.1 + (descendant_metrics.preferred_descendant_sequence_match_score * 0.24)
            );
            relation.reasons.push_back("preferred_descendant_sequence_match");
        }
        if (descendant_metrics.preferred_campaign_descendant_sequence_match_score > 0.0) {
            relation.score += std::min(
                0.36,
                0.08 + (descendant_metrics.preferred_campaign_descendant_sequence_match_score * 0.22)
            );
            relation.reasons.push_back("campaign_preferred_descendant_sequence_match");
        }
        if (descendant_metrics.preferred_portfolio_descendant_sequence_match_score > 0.0) {
            relation.score += std::min(
                0.36,
                0.08 + (descendant_metrics.preferred_portfolio_descendant_sequence_match_score * 0.22)
            );
            relation.reasons.push_back("portfolio_preferred_descendant_sequence_match");
        }
        if (descendant_metrics.descendant_focus_strength > 0.0) {
            relation.score += std::min(
                0.62,
                0.12 + (descendant_metrics.descendant_focus_strength * 0.34)
            );
            relation.reasons.push_back("descendant_focus_strength");
        }
        if (relation.score <= 0.0) {
            continue;
        }
        scored_rows.push_back({relation.score, row, descendant_metrics});
    }

    std::sort(scored_rows.begin(), scored_rows.end(), [](const auto& left, const auto& right) {
        if (left.score != right.score) {
            return left.score > right.score;
        }
        if (left.row.is_foreground != right.row.is_foreground) {
            return left.row.is_foreground;
        }
        const int left_area = std::max(0, left.row.right - left.row.left) * std::max(0, left.row.bottom - left.row.top);
        const int right_area = std::max(0, right.row.right - right.row.left) * std::max(0, right.row.bottom - right.row.top);
        if (left_area != right_area) {
            return left_area > right_area;
        }
        return left.row.title < right.row.title;
    });

    if (scored_rows.empty()) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    const ScoredRelatedWindowCandidate& selected = scored_rows.front();
    const WindowSnapshot& candidate = selected.row;
    const DescendantChainMetrics& descendant_metrics = selected.descendant_metrics;
    const double top_score = std::round(selected.score * 1000.0) / 1000.0;
    if (top_score < 0.42) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    const WindowSnapshot& adoption_target =
        descendant_metrics.preferred_descendant_found ? descendant_metrics.preferred_descendant : candidate;
    WindowSnapshot adopted_window;
    bool focus_applied = false;
    if (!focus_snapshot_target(adoption_target, adopted_window, focus_applied)) {
        return build_error_json("Related window candidate was found but could not be focused.");
    }

    int adopted_descendant_depth = 0;
    const bool adopted_is_descendant = snapshot_is_descendant_of(
        rows,
        adoption_target,
        candidate.hwnd,
        &adopted_descendant_depth
    );
    const std::string chain_signature = child_chain_signature(
        candidate.hwnd,
        static_cast<int>(descendant_metrics.direct_children.size()),
        descendant_metrics.descendant_chain_depth,
        descendant_metrics.descendant_titles
    );
    DescendantChainMetrics reported_descendant_metrics = descendant_metrics;
    WindowSnapshot reported_preferred_descendant =
        descendant_metrics.preferred_descendant_found ? descendant_metrics.preferred_descendant : WindowSnapshot{};
    int reported_adopted_descendant_depth = adopted_is_descendant ? adopted_descendant_depth : 0;
    int executed_descendant_focus_steps = adopted_is_descendant ? 1 : 0;
    int descendant_focus_chain_hops = 0;
    bool descendant_focus_chain_applied = false;
    bool descendant_focus_chain_stable = false;
    double descendant_focus_chain_quality = descendant_chain_follow_quality(descendant_metrics);
    std::string descendant_focus_chain_stop_reason =
        follow_descendant_chain_requested ? "" : "not_requested";
    bool descendant_focus_chain_anchor_recovered = false;
    int descendant_focus_chain_anchor_recovery_count = 0;
    std::string descendant_focus_chain_anchor_recovery_reason =
        follow_descendant_chain_requested ? "not_needed" : "not_requested";
    double descendant_focus_chain_anchor_recovery_match_score = 0.0;
    std::vector<std::string> descendant_focus_chain_titles;
    std::vector<long long> descendant_focus_chain_hwnds;
    std::vector<std::string> descendant_focus_chain_anchor_recovery_titles;
    std::vector<long long> descendant_focus_chain_anchor_recovery_hwnds;
    std::vector<long long> visited_descendant_hwnds;
    std::vector<std::string> visited_chain_signatures;
    if (adopted_is_descendant) {
        if (!adopted_window.title.empty()) {
            descendant_focus_chain_titles.push_back(adopted_window.title);
        }
        if (adopted_window.hwnd > 0) {
            descendant_focus_chain_hwnds.push_back(adopted_window.hwnd);
            visited_descendant_hwnds.push_back(adopted_window.hwnd);
        }
        visited_chain_signatures.push_back(chain_signature);
        reported_preferred_descendant = adopted_window;
    }

    if (follow_descendant_chain_requested) {
        if (!adopted_is_descendant) {
            descendant_focus_chain_stop_reason =
                descendant_metrics.preferred_descendant_found
                    ? "initial_focus_not_descendant"
                    : "no_initial_descendant_focus";
        } else if (safe_max_descendant_focus_steps <= executed_descendant_focus_steps) {
            descendant_focus_chain_stop_reason = "max_descendant_focus_steps_reached";
        } else {
            WindowSnapshot chain_anchor = adopted_window;
            while (executed_descendant_focus_steps < safe_max_descendant_focus_steps) {
                std::vector<WindowSnapshot> chain_rows = enumerate_window_snapshots(safe_limit);
                const WindowSnapshot* refreshed_anchor =
                    find_snapshot_by_hwnd(chain_rows, chain_anchor.hwnd);
                WindowSnapshot refreshed_anchor_storage;
                if (refreshed_anchor == nullptr) {
                    const ChainAnchorRecoveryCandidate recovered_anchor = recover_descendant_chain_anchor(
                        chain_rows,
                        chain_anchor,
                        query,
                        hint_query,
                        descendant_hint_query,
                        descendant_title_sequence,
                        campaign_hint_query,
                        campaign_preferred_title,
                        campaign_descendant_title_sequence,
                        portfolio_hint_query,
                        portfolio_preferred_title,
                        portfolio_descendant_title_sequence,
                        confirmation_hint_query,
                        confirmation_preferred_title,
                        confirmation_title_sequence,
                        preferred_title,
                        window_title,
                        reported_descendant_metrics,
                        visited_descendant_hwnds
                    );
                    if (!recovered_anchor.found) {
                        descendant_focus_chain_anchor_recovery_reason = "anchor_missing_no_same_family_match";
                        descendant_focus_chain_stop_reason = "anchor_window_missing";
                        break;
                    }
                    WindowSnapshot recovered_anchor_snapshot;
                    bool recovered_anchor_focus_applied = false;
                    if (!focus_snapshot_target(
                            recovered_anchor.snapshot,
                            recovered_anchor_snapshot,
                            recovered_anchor_focus_applied
                        )) {
                        descendant_focus_chain_anchor_recovery_reason = "anchor_recovery_focus_failed";
                        descendant_focus_chain_stop_reason = "anchor_window_missing";
                        break;
                    }
                    focus_applied = focus_applied || recovered_anchor_focus_applied;
                    adopted_window = recovered_anchor_snapshot;
                    chain_anchor = recovered_anchor_snapshot;
                    descendant_focus_chain_anchor_recovered = true;
                    ++descendant_focus_chain_anchor_recovery_count;
                    descendant_focus_chain_anchor_recovery_reason = recovered_anchor.reason;
                    descendant_focus_chain_anchor_recovery_match_score = std::max(
                        descendant_focus_chain_anchor_recovery_match_score,
                        recovered_anchor.match_score
                    );
                    if (!recovered_anchor_snapshot.title.empty()
                        && std::find(
                            descendant_focus_chain_anchor_recovery_titles.begin(),
                            descendant_focus_chain_anchor_recovery_titles.end(),
                            recovered_anchor_snapshot.title
                        ) == descendant_focus_chain_anchor_recovery_titles.end()) {
                        descendant_focus_chain_anchor_recovery_titles.push_back(recovered_anchor_snapshot.title);
                    }
                    if (recovered_anchor_snapshot.hwnd > 0
                        && std::find(
                            descendant_focus_chain_anchor_recovery_hwnds.begin(),
                            descendant_focus_chain_anchor_recovery_hwnds.end(),
                            recovered_anchor_snapshot.hwnd
                        ) == descendant_focus_chain_anchor_recovery_hwnds.end()) {
                        descendant_focus_chain_anchor_recovery_hwnds.push_back(recovered_anchor_snapshot.hwnd);
                    }
                    chain_rows = enumerate_window_snapshots(safe_limit);
                    refreshed_anchor = find_snapshot_by_hwnd(chain_rows, recovered_anchor_snapshot.hwnd);
                    if (refreshed_anchor == nullptr) {
                        refreshed_anchor_storage = recovered_anchor_snapshot;
                        refreshed_anchor = &refreshed_anchor_storage;
                    }
                }
                const DescendantChainMetrics chain_metrics = analyze_descendant_chain(
                    chain_rows,
                    *refreshed_anchor,
                    query,
                    hint_query,
                    descendant_hint_query,
                    descendant_title_sequence,
                    campaign_hint_query,
                    campaign_preferred_title,
                    campaign_descendant_title_sequence,
                    portfolio_hint_query,
                    portfolio_preferred_title,
                    portfolio_descendant_title_sequence,
                    confirmation_hint_query,
                    confirmation_preferred_title,
                    confirmation_title_sequence,
                    preferred_title,
                    window_title
                );
                reported_descendant_metrics = chain_metrics;
                descendant_focus_chain_quality = descendant_chain_follow_quality(chain_metrics);
                if (!chain_metrics.preferred_descendant_found) {
                    descendant_focus_chain_stable = true;
                    descendant_focus_chain_stop_reason = "stable_no_further_descendant";
                    break;
                }
                const std::string next_chain_signature = child_chain_signature(
                    refreshed_anchor->hwnd,
                    static_cast<int>(chain_metrics.direct_children.size()),
                    chain_metrics.descendant_chain_depth,
                    chain_metrics.descendant_titles
                );
                if (std::find(
                        visited_chain_signatures.begin(),
                        visited_chain_signatures.end(),
                        next_chain_signature
                    ) != visited_chain_signatures.end()) {
                    descendant_focus_chain_stop_reason = "repeat_child_chain_signature";
                    break;
                }
                if (!descendant_chain_follow_allowed(chain_metrics)) {
                    descendant_focus_chain_stop_reason = "chain_quality_drop";
                    break;
                }

                const WindowSnapshot& next_target = chain_metrics.preferred_descendant;
                if (std::find(
                        visited_descendant_hwnds.begin(),
                        visited_descendant_hwnds.end(),
                        next_target.hwnd
                    ) != visited_descendant_hwnds.end()) {
                    descendant_focus_chain_stop_reason = "repeat_descendant_hwnd";
                    break;
                }

                WindowSnapshot next_adopted_window;
                bool next_focus_applied = false;
                if (!focus_snapshot_target(next_target, next_adopted_window, next_focus_applied)) {
                    descendant_focus_chain_stop_reason = "follow_focus_failed";
                    break;
                }

                const std::vector<WindowSnapshot> after_focus_rows = enumerate_window_snapshots(safe_limit);
                int next_descendant_depth = 0;
                const bool next_is_descendant = snapshot_is_descendant_of(
                    after_focus_rows,
                    next_adopted_window,
                    refreshed_anchor->hwnd,
                    &next_descendant_depth
                );
                if (!next_is_descendant) {
                    descendant_focus_chain_stop_reason = "follow_focus_not_descendant";
                    break;
                }

                focus_applied = focus_applied || next_focus_applied;
                adopted_window = next_adopted_window;
                chain_anchor = next_adopted_window;
                reported_preferred_descendant = next_adopted_window;
                reported_adopted_descendant_depth = next_descendant_depth;
                ++executed_descendant_focus_steps;
                ++descendant_focus_chain_hops;
                descendant_focus_chain_applied = true;
                if (!next_adopted_window.title.empty()) {
                    descendant_focus_chain_titles.push_back(next_adopted_window.title);
                }
                if (next_adopted_window.hwnd > 0) {
                    descendant_focus_chain_hwnds.push_back(next_adopted_window.hwnd);
                    visited_descendant_hwnds.push_back(next_adopted_window.hwnd);
                }
                visited_chain_signatures.push_back(next_chain_signature);
            }
            if (descendant_focus_chain_stop_reason.empty()) {
                descendant_focus_chain_stop_reason =
                    executed_descendant_focus_steps >= safe_max_descendant_focus_steps
                        ? "max_descendant_focus_steps_reached"
                        : "stable_no_further_descendant";
            }
        }
    }

    if (!reported_preferred_descendant.hwnd && adopted_window.hwnd > 0 && adopted_is_descendant) {
        reported_preferred_descendant = adopted_window;
    }
    const bool adopted_matches_preferred_descendant = bool(
        reported_preferred_descendant.hwnd > 0
        && adopted_window.hwnd > 0
        && adopted_window.hwnd == reported_preferred_descendant.hwnd
    );
    const std::string descendant_focus_chain_signature = child_chain_signature(
        candidate.hwnd,
        static_cast<int>(descendant_focus_chain_titles.size()),
        executed_descendant_focus_steps,
        descendant_focus_chain_titles
    );
    const std::string adoption_source = descendant_focus_chain_applied
        ? "preferred_descendant_chain"
        : (descendant_metrics.preferred_descendant_found ? "preferred_descendant" : "candidate");
    const std::string adoption_transition_kind = descendant_focus_chain_applied
        ? "descendant_focus_chain"
        : (adopted_is_descendant ? "descendant_focus" : "candidate_focus");

    std::ostringstream output;
    output << "{"
           << "\"status\":\"success\","
           << "\"backend\":\"cpp_cython\","
           << "\"focus_applied\":" << (focus_applied ? "true" : "false") << ","
           << "\"adoption_source\":\"" << adoption_source << "\","
           << "\"adoption_transition_kind\":\"" << adoption_transition_kind << "\","
           << "\"candidate\":" << snapshot_to_json(candidate) << ","
           << "\"adopted_window\":" << snapshot_to_json(adopted_window) << ","
           << "\"match_score\":" << top_score << ","
           << "\"direct_child_window_count\":" << reported_descendant_metrics.direct_children.size() << ","
           << "\"direct_child_dialog_like_count\":" << reported_descendant_metrics.direct_child_dialog_like_count << ","
           << "\"descendant_chain_depth\":" << reported_descendant_metrics.descendant_chain_depth << ","
           << "\"descendant_dialog_chain_depth\":" << reported_descendant_metrics.descendant_dialog_chain_depth << ","
           << "\"descendant_query_match_count\":" << reported_descendant_metrics.descendant_query_match_count << ","
           << "\"descendant_hint_title_match_count\":" << reported_descendant_metrics.descendant_hint_title_match_count << ","
           << "\"campaign_descendant_hint_title_match_count\":" << reported_descendant_metrics.campaign_descendant_hint_title_match_count << ","
           << "\"portfolio_descendant_hint_title_match_count\":" << reported_descendant_metrics.portfolio_descendant_hint_title_match_count << ","
           << "\"confirmation_descendant_hint_title_match_count\":"
           << reported_descendant_metrics.confirmation_descendant_hint_title_match_count << ","
           << "\"descendant_sequence_match_count\":" << reported_descendant_metrics.descendant_sequence_match_count << ","
           << "\"campaign_descendant_sequence_match_count\":" << reported_descendant_metrics.campaign_descendant_sequence_match_count << ","
           << "\"portfolio_descendant_sequence_match_count\":" << reported_descendant_metrics.portfolio_descendant_sequence_match_count << ","
           << "\"confirmation_descendant_sequence_match_count\":"
           << reported_descendant_metrics.confirmation_descendant_sequence_match_count << ","
           << "\"expected_descendant_sequence_title\":\""
           << json_escape(reported_descendant_metrics.expected_descendant_sequence_title) << "\","
           << "\"expected_campaign_descendant_sequence_title\":\""
           << json_escape(reported_descendant_metrics.expected_campaign_descendant_sequence_title) << "\","
           << "\"expected_portfolio_descendant_sequence_title\":\""
           << json_escape(reported_descendant_metrics.expected_portfolio_descendant_sequence_title) << "\","
           << "\"expected_confirmation_descendant_sequence_title\":\""
           << json_escape(reported_descendant_metrics.expected_confirmation_descendant_sequence_title) << "\","
           << "\"preferred_descendant_sequence_match_score\":"
           << reported_descendant_metrics.preferred_descendant_sequence_match_score << ","
           << "\"preferred_campaign_descendant_sequence_match_score\":"
           << reported_descendant_metrics.preferred_campaign_descendant_sequence_match_score << ","
           << "\"preferred_portfolio_descendant_sequence_match_score\":"
           << reported_descendant_metrics.preferred_portfolio_descendant_sequence_match_score << ","
           << "\"preferred_confirmation_descendant_sequence_match_score\":"
           << reported_descendant_metrics.preferred_confirmation_descendant_sequence_match_score << ","
           << "\"confirmation_sequence_progress_score\":"
           << reported_descendant_metrics.confirmation_sequence_progress_score << ","
           << "\"confirmation_chain_readiness\":"
           << reported_descendant_metrics.confirmation_chain_readiness << ","
           << "\"preferred_descendant_match_score\":" << reported_descendant_metrics.preferred_descendant_match_score << ","
           << "\"descendant_focus_strength\":" << reported_descendant_metrics.descendant_focus_strength << ","
           << "\"adopted_descendant_depth\":" << reported_adopted_descendant_depth << ","
           << "\"adopted_matches_preferred_descendant\":" << (adopted_matches_preferred_descendant ? "true" : "false") << ","
           << "\"follow_descendant_chain_requested\":"
           << (follow_descendant_chain_requested ? "true" : "false") << ","
           << "\"max_descendant_focus_steps\":" << safe_max_descendant_focus_steps << ","
           << "\"descendant_focus_chain_applied\":"
           << (descendant_focus_chain_applied ? "true" : "false") << ","
           << "\"executed_descendant_focus_steps\":" << executed_descendant_focus_steps << ","
           << "\"descendant_focus_chain_hops\":" << descendant_focus_chain_hops << ","
           << "\"descendant_focus_chain_stable\":"
           << (descendant_focus_chain_stable ? "true" : "false") << ","
           << "\"descendant_focus_chain_anchor_recovered\":"
           << (descendant_focus_chain_anchor_recovered ? "true" : "false") << ","
           << "\"descendant_focus_chain_anchor_recovery_count\":"
           << descendant_focus_chain_anchor_recovery_count << ","
           << "\"descendant_focus_chain_anchor_recovery_reason\":\""
           << json_escape(descendant_focus_chain_anchor_recovery_reason) << "\","
           << "\"descendant_focus_chain_anchor_recovery_match_score\":"
           << descendant_focus_chain_anchor_recovery_match_score << ","
           << "\"descendant_focus_chain_stop_reason\":\""
           << json_escape(descendant_focus_chain_stop_reason) << "\","
           << "\"descendant_focus_chain_quality\":" << descendant_focus_chain_quality << ","
           << "\"descendant_focus_chain_signature\":\""
           << json_escape(descendant_focus_chain_signature) << "\","
           << "\"child_chain_signature\":\"" << json_escape(chain_signature) << "\","
           << "\"direct_child_titles\":[";
    for (std::size_t index = 0; index < reported_descendant_metrics.direct_child_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(reported_descendant_metrics.direct_child_titles[index]) << "\"";
    }
    output << "],\"descendant_chain_titles\":[";
    for (std::size_t index = 0; index < reported_descendant_metrics.descendant_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(reported_descendant_metrics.descendant_titles[index]) << "\"";
    }
    output << "],\"descendant_focus_chain_titles\":[";
    for (std::size_t index = 0; index < descendant_focus_chain_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(descendant_focus_chain_titles[index]) << "\"";
    }
    output << "],\"descendant_focus_chain_hwnds\":[";
    for (std::size_t index = 0; index < descendant_focus_chain_hwnds.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << descendant_focus_chain_hwnds[index];
    }
    output << "],\"descendant_focus_chain_anchor_recovery_titles\":[";
    for (std::size_t index = 0; index < descendant_focus_chain_anchor_recovery_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(descendant_focus_chain_anchor_recovery_titles[index]) << "\"";
    }
    output << "],\"descendant_focus_chain_anchor_recovery_hwnds\":[";
    for (std::size_t index = 0; index < descendant_focus_chain_anchor_recovery_hwnds.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << descendant_focus_chain_anchor_recovery_hwnds[index];
    }
    output << "],\"preferred_descendant\":";
    if (reported_preferred_descendant.hwnd > 0) {
        output << snapshot_to_json(reported_preferred_descendant);
    } else {
        output << "null";
    }
    output << "}";
    return output.str();
}

std::string reacquire_related_window_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& descendant_hint_query_utf8,
    const std::string& descendant_title_sequence_utf8,
    const std::string& campaign_hint_query_utf8,
    const std::string& campaign_preferred_title_utf8,
    const std::string& campaign_descendant_title_sequence_utf8,
    const std::string& portfolio_hint_query_utf8,
    const std::string& portfolio_preferred_title_utf8,
    const std::string& portfolio_descendant_title_sequence_utf8,
    const std::string& confirmation_hint_query_utf8,
    const std::string& confirmation_preferred_title_utf8,
    const std::string& confirmation_title_sequence_utf8,
    const std::string& preferred_title_utf8,
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
    const std::wstring descendant_hint_query = utf8_to_wide(descendant_hint_query_utf8);
    const std::vector<std::wstring> descendant_title_sequence =
        parse_title_sequence_utf8(descendant_title_sequence_utf8);
    const std::wstring campaign_hint_query = utf8_to_wide(campaign_hint_query_utf8);
    const std::wstring campaign_preferred_title = utf8_to_wide(campaign_preferred_title_utf8);
    const std::vector<std::wstring> campaign_descendant_title_sequence =
        parse_title_sequence_utf8(campaign_descendant_title_sequence_utf8);
    const std::wstring portfolio_hint_query = utf8_to_wide(portfolio_hint_query_utf8);
    const std::wstring portfolio_preferred_title = utf8_to_wide(portfolio_preferred_title_utf8);
    const std::vector<std::wstring> portfolio_descendant_title_sequence =
        parse_title_sequence_utf8(portfolio_descendant_title_sequence_utf8);
    const std::wstring confirmation_hint_query = utf8_to_wide(confirmation_hint_query_utf8);
    const std::wstring confirmation_preferred_title =
        utf8_to_wide(confirmation_preferred_title_utf8);
    const std::vector<std::wstring> confirmation_title_sequence =
        parse_title_sequence_utf8(confirmation_title_sequence_utf8);
    const std::wstring preferred_title = utf8_to_wide(preferred_title_utf8);
    const std::wstring window_title = utf8_to_wide(window_title_utf8);
    const long long anchor_hwnd = anchor_found ? anchor.hwnd : hwnd_value;
    const long anchor_pid = pid_value > 0 ? pid_value : (anchor_found ? anchor.pid : 0);
    const long long anchor_root_owner_hwnd =
        anchor_found && anchor.root_owner_hwnd > 0 ? anchor.root_owner_hwnd : (anchor_hwnd > 0 ? anchor_hwnd : 0);
    const int anchor_owner_chain_depth = anchor_found ? anchor.owner_chain_depth : 0;

    struct ScoredRelatedWindowCandidate {
        double score = 0.0;
        WindowSnapshot row;
        DescendantChainMetrics descendant_metrics;
    };
    std::vector<ScoredRelatedWindowCandidate> scored_rows;
    for (const WindowSnapshot& row : rows) {
        RelatedWindowScore relation = score_related_window(
            row,
            query,
            hint_query,
            descendant_hint_query,
            campaign_hint_query,
            campaign_preferred_title,
            portfolio_hint_query,
            portfolio_preferred_title,
            confirmation_hint_query,
            confirmation_preferred_title,
            preferred_title,
            window_title,
            anchor_hwnd,
            anchor_pid,
            anchor_root_owner_hwnd,
            anchor_owner_chain_depth
        );
        const DescendantChainMetrics descendant_metrics = analyze_descendant_chain(
            rows,
            row,
            query,
            hint_query,
            descendant_hint_query,
            descendant_title_sequence,
            campaign_hint_query,
            campaign_preferred_title,
            campaign_descendant_title_sequence,
            portfolio_hint_query,
            portfolio_preferred_title,
            portfolio_descendant_title_sequence,
            confirmation_hint_query,
            confirmation_preferred_title,
            confirmation_title_sequence,
            preferred_title,
            window_title
        );
        if (descendant_metrics.preferred_descendant_found) {
            relation.score += 0.18;
            relation.reasons.push_back("preferred_descendant_available");
        }
        if (descendant_metrics.descendant_query_match_count > 0) {
            relation.score += std::min(
                0.7,
                0.16 + (0.12 * descendant_metrics.descendant_query_match_count)
            );
            relation.reasons.push_back("descendant_query_match");
        }
        if (descendant_metrics.descendant_hint_title_match_count > 0) {
            relation.score += std::min(
                0.54,
                0.12 + (0.1 * descendant_metrics.descendant_hint_title_match_count)
            );
            relation.reasons.push_back("descendant_hint_title_match");
        }
        if (descendant_metrics.campaign_descendant_hint_title_match_count > 0) {
            relation.score += std::min(
                0.48,
                0.1 + (0.1 * descendant_metrics.campaign_descendant_hint_title_match_count)
            );
            relation.reasons.push_back("campaign_descendant_hint_title_match");
        }
        if (descendant_metrics.descendant_sequence_match_count > 0) {
            relation.score += std::min(
                0.44,
                0.1 + (0.08 * descendant_metrics.descendant_sequence_match_count)
            );
            relation.reasons.push_back("descendant_sequence_match");
        }
        if (descendant_metrics.campaign_descendant_sequence_match_count > 0) {
            relation.score += std::min(
                0.38,
                0.08 + (0.07 * descendant_metrics.campaign_descendant_sequence_match_count)
            );
            relation.reasons.push_back("campaign_descendant_sequence_match");
        }
        if (descendant_metrics.preferred_descendant_match_score > 0.0) {
            relation.score += std::min(
                0.46,
                0.12 + (descendant_metrics.preferred_descendant_match_score * 0.28)
            );
            relation.reasons.push_back("preferred_descendant_match");
        }
        if (descendant_metrics.preferred_descendant_sequence_match_score > 0.0) {
            relation.score += std::min(
                0.42,
                0.1 + (descendant_metrics.preferred_descendant_sequence_match_score * 0.24)
            );
            relation.reasons.push_back("preferred_descendant_sequence_match");
        }
        if (descendant_metrics.preferred_campaign_descendant_sequence_match_score > 0.0) {
            relation.score += std::min(
                0.36,
                0.08 + (descendant_metrics.preferred_campaign_descendant_sequence_match_score * 0.22)
            );
            relation.reasons.push_back("campaign_preferred_descendant_sequence_match");
        }
        if (descendant_metrics.descendant_focus_strength > 0.0) {
            relation.score += std::min(
                0.62,
                0.12 + (descendant_metrics.descendant_focus_strength * 0.34)
            );
            relation.reasons.push_back("descendant_focus_strength");
        }
        if (relation.score <= 0.0) {
            continue;
        }
        scored_rows.push_back({relation.score, row, descendant_metrics});
    }

    std::sort(scored_rows.begin(), scored_rows.end(), [](const auto& left, const auto& right) {
        if (left.score != right.score) {
            return left.score > right.score;
        }
        if (left.row.is_foreground != right.row.is_foreground) {
            return left.row.is_foreground;
        }
        const int left_area = std::max(0, left.row.right - left.row.left) * std::max(0, left.row.bottom - left.row.top);
        const int right_area = std::max(0, right.row.right - right.row.left) * std::max(0, right.row.bottom - right.row.top);
        if (left_area != right_area) {
            return left_area > right_area;
        }
        return left.row.title < right.row.title;
    });

    if (scored_rows.empty()) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    std::vector<WindowSnapshot> candidates;
    candidates.reserve(std::min<std::size_t>(8, scored_rows.size()));
    for (std::size_t index = 0; index < scored_rows.size() && index < 8; ++index) {
        candidates.push_back(scored_rows[index].row);
    }
    const ScoredRelatedWindowCandidate& selected = scored_rows.front();
    const WindowSnapshot& candidate = selected.row;
    const DescendantChainMetrics& descendant_metrics = selected.descendant_metrics;
    const double top_score = std::round(selected.score * 1000.0) / 1000.0;
    if (top_score < 0.42) {
        return "{\"status\":\"missing\",\"backend\":\"cpp_cython\",\"message\":\"no matching related window candidate found\"}";
    }

    return related_window_payload_json(
        rows,
        candidate,
        candidates,
        descendant_metrics,
        candidate.root_owner_hwnd > 0 ? candidate.root_owner_hwnd : anchor_root_owner_hwnd,
        candidate.pid > 0 ? candidate.pid : anchor_pid,
        top_score
    );
}

std::string trace_related_window_chain_json(
    const std::string& query_utf8,
    const std::string& hint_query_utf8,
    const std::string& descendant_hint_query_utf8,
    const std::string& descendant_title_sequence_utf8,
    const std::string& campaign_hint_query_utf8,
    const std::string& campaign_preferred_title_utf8,
    const std::string& campaign_descendant_title_sequence_utf8,
    const std::string& portfolio_hint_query_utf8,
    const std::string& portfolio_preferred_title_utf8,
    const std::string& portfolio_descendant_title_sequence_utf8,
    const std::string& confirmation_hint_query_utf8,
    const std::string& confirmation_preferred_title_utf8,
    const std::string& confirmation_title_sequence_utf8,
    const std::string& preferred_title_utf8,
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
    const std::wstring descendant_hint_query = utf8_to_wide(descendant_hint_query_utf8);
    const std::vector<std::wstring> descendant_title_sequence =
        parse_title_sequence_utf8(descendant_title_sequence_utf8);
    const std::wstring campaign_hint_query = utf8_to_wide(campaign_hint_query_utf8);
    const std::wstring campaign_preferred_title = utf8_to_wide(campaign_preferred_title_utf8);
    const std::vector<std::wstring> campaign_descendant_title_sequence =
        parse_title_sequence_utf8(campaign_descendant_title_sequence_utf8);
    const std::wstring portfolio_hint_query = utf8_to_wide(portfolio_hint_query_utf8);
    const std::wstring portfolio_preferred_title = utf8_to_wide(portfolio_preferred_title_utf8);
    const std::vector<std::wstring> portfolio_descendant_title_sequence =
        parse_title_sequence_utf8(portfolio_descendant_title_sequence_utf8);
    const std::wstring confirmation_hint_query = utf8_to_wide(confirmation_hint_query_utf8);
    const std::wstring confirmation_preferred_title =
        utf8_to_wide(confirmation_preferred_title_utf8);
    const std::vector<std::wstring> confirmation_title_sequence =
        parse_title_sequence_utf8(confirmation_title_sequence_utf8);
    const std::wstring preferred_title = utf8_to_wide(preferred_title_utf8);
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
            descendant_hint_query,
            campaign_hint_query,
            campaign_preferred_title,
            portfolio_hint_query,
            portfolio_preferred_title,
            confirmation_hint_query,
            confirmation_preferred_title,
            preferred_title,
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

    const DescendantChainMetrics descendant_metrics = analyze_descendant_chain(
        rows,
        candidate,
        query,
        hint_query,
        descendant_hint_query,
        descendant_title_sequence,
        campaign_hint_query,
        campaign_preferred_title,
        campaign_descendant_title_sequence,
        portfolio_hint_query,
        portfolio_preferred_title,
        portfolio_descendant_title_sequence,
        confirmation_hint_query,
        confirmation_preferred_title,
        confirmation_title_sequence,
        preferred_title,
        window_title
    );

    const std::string chain_signature = child_chain_signature(
        candidate.hwnd,
        static_cast<int>(descendant_metrics.direct_children.size()),
        descendant_metrics.descendant_chain_depth,
        descendant_metrics.descendant_titles
    );

    std::ostringstream output;
    output << "{"
           << "\"status\":\"success\","
           << "\"backend\":\"cpp_cython\","
           << "\"candidate\":" << snapshot_to_json(candidate) << ","
           << "\"match_score\":" << top_score << ","
           << "\"direct_child_window_count\":" << descendant_metrics.direct_children.size() << ","
           << "\"direct_child_dialog_like_count\":" << descendant_metrics.direct_child_dialog_like_count << ","
           << "\"descendant_chain_depth\":" << descendant_metrics.descendant_chain_depth << ","
           << "\"descendant_dialog_chain_depth\":" << descendant_metrics.descendant_dialog_chain_depth << ","
           << "\"descendant_query_match_count\":" << descendant_metrics.descendant_query_match_count << ","
           << "\"descendant_hint_title_match_count\":" << descendant_metrics.descendant_hint_title_match_count << ","
           << "\"campaign_descendant_hint_title_match_count\":"
           << descendant_metrics.campaign_descendant_hint_title_match_count << ","
           << "\"portfolio_descendant_hint_title_match_count\":"
           << descendant_metrics.portfolio_descendant_hint_title_match_count << ","
           << "\"confirmation_descendant_hint_title_match_count\":"
           << descendant_metrics.confirmation_descendant_hint_title_match_count << ","
           << "\"descendant_sequence_match_count\":"
           << descendant_metrics.descendant_sequence_match_count << ","
           << "\"campaign_descendant_sequence_match_count\":"
           << descendant_metrics.campaign_descendant_sequence_match_count << ","
           << "\"portfolio_descendant_sequence_match_count\":"
           << descendant_metrics.portfolio_descendant_sequence_match_count << ","
           << "\"confirmation_descendant_sequence_match_count\":"
           << descendant_metrics.confirmation_descendant_sequence_match_count << ","
           << "\"expected_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_descendant_sequence_title) << "\","
           << "\"expected_campaign_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_campaign_descendant_sequence_title) << "\","
           << "\"expected_portfolio_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_portfolio_descendant_sequence_title) << "\","
           << "\"expected_confirmation_descendant_sequence_title\":\""
           << json_escape(descendant_metrics.expected_confirmation_descendant_sequence_title) << "\","
           << "\"preferred_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_descendant_sequence_match_score << ","
           << "\"preferred_campaign_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_campaign_descendant_sequence_match_score << ","
           << "\"preferred_portfolio_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_portfolio_descendant_sequence_match_score << ","
           << "\"preferred_confirmation_descendant_sequence_match_score\":"
           << descendant_metrics.preferred_confirmation_descendant_sequence_match_score << ","
           << "\"confirmation_sequence_progress_score\":"
           << descendant_metrics.confirmation_sequence_progress_score << ","
           << "\"confirmation_chain_readiness\":"
           << descendant_metrics.confirmation_chain_readiness << ","
           << "\"preferred_descendant_match_score\":" << descendant_metrics.preferred_descendant_match_score << ","
           << "\"descendant_focus_strength\":" << descendant_metrics.descendant_focus_strength << ","
           << "\"child_chain_signature\":\"" << json_escape(chain_signature) << "\","
           << "\"direct_child_titles\":[";
    for (std::size_t index = 0; index < descendant_metrics.direct_child_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(descendant_metrics.direct_child_titles[index]) << "\"";
    }
    output << "],\"descendant_chain_titles\":[";
    for (std::size_t index = 0; index < descendant_metrics.descendant_titles.size(); ++index) {
        if (index > 0) {
            output << ",";
        }
        output << "\"" << json_escape(descendant_metrics.descendant_titles[index]) << "\"";
    }
    output << "],\"preferred_descendant\":";
    if (descendant_metrics.preferred_descendant_found) {
        output << snapshot_to_json(descendant_metrics.preferred_descendant);
    } else {
        output << "null";
    }
    output << "}";
    return output.str();
}

}  // namespace jarvis::native
