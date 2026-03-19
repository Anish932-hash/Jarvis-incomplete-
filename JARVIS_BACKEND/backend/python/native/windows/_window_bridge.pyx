# cython: language_level=3

from libcpp.string cimport string

import json


cdef extern from "window_bridge.hpp" namespace "jarvis::native":
    string list_windows_json(int limit) except +
    string active_window_json() except +
    string focus_window_json(const string& title_contains_utf8, long long hwnd_value) except +
    string reacquire_related_window_json(
        const string& query_utf8,
        const string& hint_query_utf8,
        const string& descendant_hint_query_utf8,
        const string& campaign_hint_query_utf8,
        const string& campaign_preferred_title_utf8,
        const string& preferred_title_utf8,
        const string& window_title_utf8,
        long long hwnd_value,
        long pid_value,
        int limit
    ) except +
    string trace_related_window_chain_json(
        const string& query_utf8,
        const string& hint_query_utf8,
        const string& descendant_hint_query_utf8,
        const string& campaign_hint_query_utf8,
        const string& campaign_preferred_title_utf8,
        const string& preferred_title_utf8,
        const string& window_title_utf8,
        long long hwnd_value,
        long pid_value,
        int limit
    ) except +


cdef object _decode_payload(const string& payload):
    cdef bytes payload_bytes = payload.c_str()[:payload.size()]
    return json.loads(payload_bytes.decode("utf-8"))


def list_windows(int limit=120):
    return _decode_payload(list_windows_json(limit))


def active_window():
    return _decode_payload(active_window_json())


def focus_window(title_contains="", hwnd=0):
    cdef string encoded_title = str(title_contains or "").encode("utf-8")
    cdef long long hwnd_value = int(hwnd or 0)
    return _decode_payload(focus_window_json(encoded_title, hwnd_value))


def reacquire_related_window(query="", hint_query="", descendant_hint_query="", campaign_hint_query="", campaign_preferred_title="", preferred_title="", window_title="", hwnd=0, pid=0, limit=120):
    cdef string encoded_query = str(query or "").encode("utf-8")
    cdef string encoded_hint_query = str(hint_query or "").encode("utf-8")
    cdef string encoded_descendant_hint_query = str(descendant_hint_query or "").encode("utf-8")
    cdef string encoded_campaign_hint_query = str(campaign_hint_query or "").encode("utf-8")
    cdef string encoded_campaign_preferred_title = str(campaign_preferred_title or "").encode("utf-8")
    cdef string encoded_preferred_title = str(preferred_title or "").encode("utf-8")
    cdef string encoded_window_title = str(window_title or "").encode("utf-8")
    cdef long long hwnd_value = int(hwnd or 0)
    cdef long pid_value = int(pid or 0)
    cdef int safe_limit = max(1, min(int(limit or 120), 500))
    return _decode_payload(
        reacquire_related_window_json(
            encoded_query,
            encoded_hint_query,
            encoded_descendant_hint_query,
            encoded_campaign_hint_query,
            encoded_campaign_preferred_title,
            encoded_preferred_title,
            encoded_window_title,
            hwnd_value,
            pid_value,
            safe_limit,
        )
    )


def trace_related_window_chain(query="", hint_query="", descendant_hint_query="", campaign_hint_query="", campaign_preferred_title="", preferred_title="", window_title="", hwnd=0, pid=0, limit=120):
    cdef string encoded_query = str(query or "").encode("utf-8")
    cdef string encoded_hint_query = str(hint_query or "").encode("utf-8")
    cdef string encoded_descendant_hint_query = str(descendant_hint_query or "").encode("utf-8")
    cdef string encoded_campaign_hint_query = str(campaign_hint_query or "").encode("utf-8")
    cdef string encoded_campaign_preferred_title = str(campaign_preferred_title or "").encode("utf-8")
    cdef string encoded_preferred_title = str(preferred_title or "").encode("utf-8")
    cdef string encoded_window_title = str(window_title or "").encode("utf-8")
    cdef long long hwnd_value = int(hwnd or 0)
    cdef long pid_value = int(pid or 0)
    cdef int safe_limit = max(1, min(int(limit or 120), 500))
    return _decode_payload(
        trace_related_window_chain_json(
            encoded_query,
            encoded_hint_query,
            encoded_descendant_hint_query,
            encoded_campaign_hint_query,
            encoded_campaign_preferred_title,
            encoded_preferred_title,
            encoded_window_title,
            hwnd_value,
            pid_value,
            safe_limit,
        )
    )
