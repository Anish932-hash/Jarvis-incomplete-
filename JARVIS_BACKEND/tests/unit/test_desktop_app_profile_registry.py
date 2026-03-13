from __future__ import annotations

from pathlib import Path

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry


def test_desktop_app_profile_registry_parses_catalog_and_categories(tmp_path: Path) -> None:
    apps_a = tmp_path / "apps-a.txt"
    apps_b = tmp_path / "apps-b.txt"
    apps_a.write_text(
        "\n".join(
            [
                "Name                                      Id                           Version    Available  Source",
                "---------------------------------------------------------------------------------------------------",
                "Google Chrome                             Google.Chrome.EXE            145.0                winget",
                "Cloudflare WARP                           Cloudflare.Warp              25.10.186             winget",
                "Warp                                      Warp.Warp                    v0.2026              winget",
                "Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget",
                "Slack                                     SlackTechnologies.Slack     4.41.105             winget",
                "Microsoft Word                            Microsoft.Office.Word       2502.0               winget",
                "Microsoft PowerPoint                      Microsoft.Office.PowerPoint 2502.0               winget",
            ]
        ),
        encoding="utf-8",
    )
    apps_b.write_text(
        "\n".join(
            [
                "Name                                      Id                           Version    Available  Source",
                "---------------------------------------------------------------------------------------------------",
                "Discord                                   ARP\\User\\X64\\Discord       1.0.9225",
                "Google Chrome                             Google.Chrome.EXE            145.0                winget",
            ]
        ),
        encoding="utf-8",
    )

    registry = DesktopAppProfileRegistry(source_paths=[str(apps_a), str(apps_b)])
    catalog = registry.catalog(limit=10)

    assert catalog["status"] == "success"
    assert catalog["total"] == 8
    assert catalog["category_counts"]["browser"] == 1
    assert catalog["category_counts"]["chat"] == 2
    assert catalog["category_counts"]["office"] == 2
    chrome = registry.match(app_name="Google Chrome")
    assert chrome["category"] == "browser"
    assert chrome["workflow_defaults"]["navigation_hotkeys"][0] == ["ctrl", "l"]
    assert chrome["workflow_capabilities"]["focus_search_box"]["primary_hotkey"] == ["ctrl", "f"]
    assert chrome["workflow_capabilities"]["new_tab"]["supported"] is True
    assert chrome["workflow_capabilities"]["new_tab"]["primary_hotkey"] == ["ctrl", "t"]
    assert chrome["workflow_capabilities"]["open_tab_search"]["primary_hotkey"] == ["ctrl", "shift", "a"]
    assert chrome["workflow_capabilities"]["search_tabs"]["supported"] is True
    assert chrome["workflow_capabilities"]["go_back"]["primary_hotkey"] == ["alt", "left"]
    assert chrome["workflow_capabilities"]["go_forward"]["primary_hotkey"] == ["alt", "right"]
    assert chrome["workflow_capabilities"]["open_bookmarks"]["primary_hotkey"] == ["ctrl", "shift", "o"]
    assert chrome["workflow_capabilities"]["open_history"]["primary_hotkey"] == ["ctrl", "h"]
    assert registry.match(app_name="Cloudflare WARP")["category"] == "security"
    warp = registry.match(app_name="Warp")
    assert warp["category"] == "terminal"
    assert warp["workflow_capabilities"]["terminal_command"]["supports_direct_input"] is True
    vscode = registry.match(app_name="Visual Studio Code")
    assert vscode["workflow_capabilities"]["focus_explorer"]["primary_hotkey"] == ["ctrl", "shift", "e"]
    assert vscode["workflow_capabilities"]["workspace_search"]["primary_hotkey"] == ["ctrl", "shift", "f"]
    assert vscode["workflow_capabilities"]["find_replace"]["primary_hotkey"] == ["ctrl", "h"]
    assert vscode["workflow_capabilities"]["go_to_symbol"]["primary_hotkey"] == ["ctrl", "shift", "o"]
    assert vscode["workflow_capabilities"]["rename_symbol"]["primary_hotkey"] == ["f2"]
    assert vscode["workflow_capabilities"]["toggle_terminal"]["supported"] is True
    assert vscode["workflow_capabilities"]["format_document"]["primary_hotkey"] == ["shift", "alt", "f"]
    explorer = registry.match(app_name="File Explorer")
    assert explorer["category"] == "file_manager"
    assert explorer["workflow_capabilities"]["focus_search_box"]["primary_hotkey"] == ["ctrl", "e"]
    assert explorer["workflow_capabilities"]["focus_address_bar"]["primary_hotkey"] == ["ctrl", "l"]
    assert explorer["workflow_capabilities"]["go_back"]["primary_hotkey"] == ["alt", "left"]
    assert explorer["workflow_capabilities"]["go_forward"]["primary_hotkey"] == ["alt", "right"]
    assert explorer["workflow_capabilities"]["new_tab"]["primary_hotkey"] == ["ctrl", "t"]
    assert explorer["workflow_capabilities"]["switch_tab"]["primary_hotkey"] == ["ctrl", "tab"]
    assert explorer["workflow_capabilities"]["focus_folder_tree"]["supported"] is True
    assert explorer["workflow_capabilities"]["focus_folder_tree"]["supports_action_dispatch"] is True
    assert explorer["workflow_capabilities"]["focus_file_list"]["supported"] is True
    assert explorer["workflow_capabilities"]["focus_file_list"]["supports_action_dispatch"] is True
    assert explorer["workflow_capabilities"]["new_folder"]["primary_hotkey"] == ["ctrl", "shift", "n"]
    assert explorer["workflow_capabilities"]["rename_selection"]["primary_hotkey"] == ["f2"]
    assert explorer["workflow_capabilities"]["open_properties_dialog"]["primary_hotkey"] == ["alt", "enter"]
    assert explorer["workflow_capabilities"]["open_preview_pane"]["primary_hotkey"] == ["alt", "p"]
    assert explorer["workflow_capabilities"]["open_details_pane"]["primary_hotkey"] == ["alt", "shift", "p"]
    assert explorer["workflow_capabilities"]["go_up_level"]["primary_hotkey"] == ["alt", "up"]
    discord = registry.match(exe_name="discord.exe")
    assert discord["status"] == "success"
    assert discord["category"] == "chat"
    slack = registry.match(app_name="Slack")
    assert slack["category"] == "chat"
    assert slack["workflow_capabilities"]["new_chat"]["primary_hotkey"] == ["ctrl", "n"]
    assert slack["workflow_capabilities"]["jump_to_conversation"]["primary_hotkey"] == ["ctrl", "k"]
    assert slack["workflow_capabilities"]["send_message"]["supports_direct_input"] is True
    word = registry.match(app_name="Microsoft Word")
    assert word["category"] == "office"
    assert word["workflow_capabilities"]["find_replace"]["primary_hotkey"] == ["ctrl", "h"]
    assert word["workflow_capabilities"]["new_document"]["primary_hotkey"] == ["ctrl", "n"]
    assert word["workflow_capabilities"]["save_document"]["primary_hotkey"] == ["ctrl", "s"]
    assert word["workflow_capabilities"]["open_print_dialog"]["primary_hotkey"] == ["ctrl", "p"]
    powerpoint = registry.match(app_name="PowerPoint")
    assert powerpoint["category"] == "office"
    assert powerpoint["workflow_capabilities"]["start_presentation"]["primary_hotkey"] == ["f5"]


def test_desktop_app_profile_registry_recognizes_extended_app_overrides(tmp_path: Path) -> None:
    apps = tmp_path / "apps.txt"
    apps.write_text(
        "\n".join(
            [
                "Name                                      Id                           Version    Available  Source",
                "---------------------------------------------------------------------------------------------------",
                "Mozilla Firefox                           Mozilla.Firefox              146.0                winget",
                "Cursor                                    Cursor.Cursor                0.51.0               winget",
                "Windows Settings                          Microsoft.WindowsSettings   1.0                  winget",
                "Task Manager                              Microsoft.TaskManager       11.0                 winget",
                "Adobe Acrobat Reader                      Adobe.Acrobat.Reader        2026.001             winget",
                "Spotify                                   Spotify.Spotify             1.2.71               winget",
                "VLC media player                          VideoLAN.VLC                3.0.21               winget",
                "OBS Studio                                OBSProject.OBSStudio        31.0                 winget",
                "Outlook for Windows                       Microsoft.Outlook           1.2026               winget",
            ]
        ),
        encoding="utf-8",
    )

    registry = DesktopAppProfileRegistry(source_paths=[str(apps)])

    firefox = registry.match(app_name="Firefox")
    assert firefox["category"] == "browser"
    assert firefox["workflow_capabilities"]["switch_tab"]["supported"] is True
    assert firefox["workflow_capabilities"]["switch_tab"]["primary_hotkey"] == ["ctrl", "tab"]
    assert firefox["workflow_capabilities"]["zoom_in"]["primary_hotkey"] == ["ctrl", "equal"]

    cursor = registry.match(app_name="Cursor")
    assert cursor["category"] == "code_editor"
    assert cursor["workflow_capabilities"]["switch_tab"]["supported"] is True
    assert cursor["workflow_capabilities"]["zoom_out"]["primary_hotkey"] == ["ctrl", "minus"]

    settings = registry.match(app_name="Settings")
    assert settings["category"] == "utility"
    assert settings["workflow_capabilities"]["switch_tab"]["supported"] is True
    assert settings["workflow_capabilities"]["switch_tab"]["primary_hotkey"] == ["ctrl", "tab"]
    assert settings["workflow_capabilities"]["focus_sidebar"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_sidebar"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["select_sidebar_item"]["supported"] is True
    assert settings["workflow_capabilities"]["select_sidebar_item"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["focus_list_surface"]["supported"] is True
    assert settings["workflow_capabilities"]["select_list_item"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_main_content"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_toolbar"]["supported"] is True
    assert settings["workflow_capabilities"]["invoke_toolbar_action"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_form_surface"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_form_surface"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["focus_input_field"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_input_field"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["set_field_value"]["supported"] is True
    assert settings["workflow_capabilities"]["set_field_value"]["supports_action_dispatch"] is True

    installer = registry.match(app_name="installer")
    assert installer["category"] == "utility"
    assert installer["workflow_capabilities"]["complete_wizard_page"]["supported"] is True
    assert installer["workflow_capabilities"]["complete_wizard_flow"]["supported"] is True
    assert settings["workflow_capabilities"]["open_dropdown"]["supported"] is True
    assert settings["workflow_capabilities"]["open_dropdown"]["primary_hotkey"] == ["alt", "down"]
    assert settings["workflow_capabilities"]["select_dropdown_option"]["supported"] is True
    assert settings["workflow_capabilities"]["select_dropdown_option"]["primary_hotkey"] == ["alt", "down"]
    assert settings["workflow_capabilities"]["focus_checkbox"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_checkbox"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["check_checkbox"]["supported"] is True
    assert settings["workflow_capabilities"]["check_checkbox"]["primary_hotkey"] == ["space"]
    assert settings["workflow_capabilities"]["uncheck_checkbox"]["supported"] is True
    assert settings["workflow_capabilities"]["uncheck_checkbox"]["primary_hotkey"] == ["space"]
    assert settings["workflow_capabilities"]["select_radio_option"]["supported"] is True
    assert settings["workflow_capabilities"]["select_radio_option"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["focus_value_control"]["supported"] is True
    assert settings["workflow_capabilities"]["focus_value_control"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["increase_value"]["supported"] is True
    assert settings["workflow_capabilities"]["increase_value"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["decrease_value"]["supported"] is True
    assert settings["workflow_capabilities"]["decrease_value"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["set_value_control"]["supported"] is True
    assert settings["workflow_capabilities"]["set_value_control"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["toggle_switch"]["supported"] is True
    assert settings["workflow_capabilities"]["toggle_switch"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["enable_switch"]["supported"] is True
    assert settings["workflow_capabilities"]["enable_switch"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["disable_switch"]["supported"] is True
    assert settings["workflow_capabilities"]["disable_switch"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["complete_form_page"]["supported"] is True
    assert settings["workflow_capabilities"]["complete_form_page"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["complete_form_flow"]["supported"] is True
    assert settings["workflow_capabilities"]["open_context_menu"]["primary_hotkey"] == ["shift", "f10"]
    assert settings["workflow_capabilities"]["select_context_menu_item"]["supported"] is True
    assert settings["workflow_capabilities"]["select_context_menu_item"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["dismiss_dialog"]["primary_hotkey"] == ["esc"]
    assert settings["workflow_capabilities"]["confirm_dialog"]["primary_hotkey"] == ["enter"]
    assert settings["workflow_capabilities"]["press_dialog_button"]["supported"] is True
    assert settings["workflow_capabilities"]["press_dialog_button"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["next_wizard_step"]["supported"] is True
    assert settings["workflow_capabilities"]["next_wizard_step"]["primary_hotkey"] == ["alt", "n"]
    assert settings["workflow_capabilities"]["next_wizard_step"]["supports_action_dispatch"] is True
    assert settings["workflow_capabilities"]["previous_wizard_step"]["supported"] is True
    assert settings["workflow_capabilities"]["previous_wizard_step"]["primary_hotkey"] == ["alt", "b"]
    assert settings["workflow_capabilities"]["finish_wizard"]["supported"] is True
    assert settings["workflow_capabilities"]["finish_wizard"]["primary_hotkey"] == ["alt", "f"]

    task_manager = registry.match(app_name="Task Manager")
    assert task_manager["category"] == "ops_console"
    assert task_manager["workflow_capabilities"]["switch_tab"]["supported"] is True
    assert task_manager["workflow_capabilities"]["switch_tab"]["primary_hotkey"] == ["ctrl", "tab"]
    assert task_manager["workflow_capabilities"]["focus_sidebar"]["supported"] is True
    assert task_manager["workflow_capabilities"]["select_sidebar_item"]["supported"] is True
    assert task_manager["workflow_capabilities"]["focus_data_table"]["supported"] is True
    assert task_manager["workflow_capabilities"]["select_table_row"]["supported"] is True
    assert task_manager["workflow_capabilities"]["select_tab_page"]["supported"] is True
    assert task_manager["workflow_capabilities"]["select_tab_page"]["supports_action_dispatch"] is True
    assert task_manager["workflow_capabilities"]["focus_main_content"]["supported"] is True
    assert task_manager["workflow_capabilities"]["invoke_toolbar_action"]["supported"] is True
    assert task_manager["workflow_capabilities"]["open_context_menu"]["primary_hotkey"] == ["shift", "f10"]
    assert task_manager["workflow_capabilities"]["select_context_menu_item"]["supported"] is True
    assert task_manager["workflow_capabilities"]["press_dialog_button"]["supported"] is True

    assert task_manager["workflow_capabilities"]["focus_navigation_tree"]["supported"] is True
    assert task_manager["workflow_capabilities"]["select_tree_item"]["supported"] is True
    assert task_manager["workflow_capabilities"]["expand_tree_item"]["supported"] is True

    installer = registry.match(app_name="Installer")
    assert installer["category"] == "utility"
    assert installer["workflow_capabilities"]["next_wizard_step"]["supported"] is True
    assert installer["workflow_capabilities"]["next_wizard_step"]["primary_hotkey"] == ["alt", "n"]
    assert installer["workflow_capabilities"]["previous_wizard_step"]["primary_hotkey"] == ["alt", "b"]
    assert installer["workflow_capabilities"]["finish_wizard"]["primary_hotkey"] == ["alt", "f"]
    assert installer["workflow_capabilities"]["complete_form_page"]["supported"] is True
    assert installer["workflow_capabilities"]["complete_form_flow"]["supported"] is True

    acrobat = registry.match(app_name="Acrobat Reader")
    assert acrobat["category"] == "utility"
    assert acrobat["workflow_capabilities"]["zoom_in"]["supported"] is True
    assert acrobat["workflow_capabilities"]["reset_zoom"]["primary_hotkey"] == ["ctrl", "0"]

    spotify = registry.match(app_name="Spotify")
    assert spotify["category"] == "media"
    assert spotify["workflow_capabilities"]["play_pause_media"]["supported"] is True
    assert spotify["workflow_capabilities"]["play_pause_media"]["supports_system_action"] is True
    assert spotify["workflow_capabilities"]["next_track"]["supported"] is True
    assert spotify["workflow_capabilities"]["stop_media"]["supported"] is True

    vlc = registry.match(app_name="VLC")
    assert vlc["category"] == "media"
    assert vlc["workflow_capabilities"]["pause_media"]["supports_system_action"] is True
    assert vlc["workflow_capabilities"]["previous_track"]["supported"] is True

    obs = registry.match(app_name="OBS")
    assert obs["category"] == "media"
    assert obs["workflow_capabilities"]["play_pause_media"]["supported"] is True

    outlook = registry.match(app_name="Outlook")
    assert outlook["category"] == "office"
    assert outlook["workflow_capabilities"]["new_email_draft"]["supported"] is True
    assert outlook["workflow_capabilities"]["new_email_draft"]["primary_hotkey"] == ["ctrl", "n"]
    assert outlook["workflow_capabilities"]["open_mail_view"]["primary_hotkey"] == ["ctrl", "1"]
    assert outlook["workflow_capabilities"]["open_calendar_view"]["primary_hotkey"] == ["ctrl", "2"]
    assert outlook["workflow_capabilities"]["open_people_view"]["primary_hotkey"] == ["ctrl", "3"]
    assert outlook["workflow_capabilities"]["open_tasks_view"]["primary_hotkey"] == ["ctrl", "4"]
    assert outlook["workflow_capabilities"]["focus_folder_pane"]["supported"] is True
    assert outlook["workflow_capabilities"]["focus_folder_pane"]["supports_action_dispatch"] is True
    assert outlook["workflow_capabilities"]["focus_message_list"]["supported"] is True
    assert outlook["workflow_capabilities"]["focus_message_list"]["supports_action_dispatch"] is True
    assert outlook["workflow_capabilities"]["focus_reading_pane"]["supported"] is True
    assert outlook["workflow_capabilities"]["focus_reading_pane"]["supports_action_dispatch"] is True
    assert outlook["workflow_capabilities"]["reply_email"]["primary_hotkey"] == ["ctrl", "r"]
    assert outlook["workflow_capabilities"]["reply_all_email"]["primary_hotkey"] == ["ctrl", "shift", "r"]
    assert outlook["workflow_capabilities"]["forward_email"]["primary_hotkey"] == ["ctrl", "f"]
    assert outlook["workflow_capabilities"]["new_calendar_event"]["primary_hotkey"] == ["ctrl", "shift", "a"]
