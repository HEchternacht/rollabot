#!/usr/bin/env python3
"""
echterbotv2 - TS3Bot Control Panel
Entry point with GUI for controlling the TeamSpeak bot.
"""

import sys
import os
import ctypes
import logging
import threading
import time
import datetime
import traceback

# Add src to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog as sd

try:
    from ttkthemes import ThemedTk
except ImportError:
    # Fallback if ttkthemes not installed
    class ThemedTk(tk.Tk):
        def __init__(self, theme=None, **kwargs):
            super().__init__(**kwargs)

try:
    from PIL import Image, ImageTk
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

from tsbotrpi.bot import TS3Bot
from tsbotrpi.config import load_config
from tsbotrpi.tsclient import TSClientManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hotkey helpers
# ---------------------------------------------------------------------------

def is_capslock_on() -> bool:
    """Return True if CapsLock is currently toggled on."""
    hllDll = ctypes.WinDLL("User32.dll")
    return hllDll.GetKeyState(0x14) & 0xFFFF != 0


def is_WASDQEZC_pressed() -> bool:
    """Return True if any of W/A/S/D/Q/E/Z/C is currently held down."""
    dll = ctypes.WinDLL("User32.dll")
    keys = [0x57, 0x41, 0x53, 0x44, 0x45, 0x5A, 0x43, 0x51]  # W A S D E Z C Q
    return any((dll.GetAsyncKeyState(k) & 0x8000) != 0 for k in keys)


def get_pressed_hotkey() -> str | None:
    """Return the letter of the pressed hotkey key (if any), else None."""
    dll = ctypes.WinDLL("User32.dll")
    key_map = {
        0x57: 'W',
        0x41: 'A',
        0x53: 'S',
        0x44: 'D',
        0x45: 'E',
        0x5A: 'Z',
        0x43: 'C',
        0x51: 'Q',
    }
    for vk, letter in key_map.items():
        if (dll.GetAsyncKeyState(vk) & 0x8000) != 0:
            return letter
    return None


# ---------------------------------------------------------------------------
# Tooltip helper
# ---------------------------------------------------------------------------

class ToolTip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tip_window = None
        widget.bind("<Enter>", self.show)
        widget.bind("<Leave>", self.hide)

    def show(self, event=None):
        if self.tip_window or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 4
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify='left',
                         background="#ffffe0", relief='solid', borderwidth=1,
                         font=('Arial', 8))
        label.pack(ipadx=4, ipady=2)

    def hide(self, event=None):
        if self.tip_window:
            self.tip_window.destroy()
            self.tip_window = None


# ---------------------------------------------------------------------------
# Main UI class
# ---------------------------------------------------------------------------

class botUI:
    """echterbotv2 Control Panel for TS3Bot."""

    # Hotkey → (description, action_name)
    HOTKEY_MAP = {
        'W': ('Connect / Start bot',     'hotkey_start'),
        'S': ('Stop bot',                'hotkey_stop'),
        'D': ('Disconnect',              'hotkey_disconnect'),
        'A': ('Reconnect',               'hotkey_reconnect'),
        'E': ('Show War Stats',          'hotkey_war_stats'),
        'Z': ('Mass-poke online users',  'hotkey_masspoke'),
        'C': ('Refresh client list',     'hotkey_refresh_clients'),
        'Q': ('Quit application',        'hotkey_quit'),
    }

    def __init__(self, bot: TS3Bot, config: dict):
        self.bot = bot
        self.config = config
        self._bot_thread: threading.Thread | None = None
        self._running = False

        # Hotkey state
        self._last_hotkey_key: str | None = None
        self._hotkey_thread: threading.Thread | None = None

        # Build window
        self.root = ThemedTk(theme="adapta")
        self.root.title("EchterX Control Panel v2")
        self.root.geometry("750x780")

        # Background image (optional)
        self.background_label = None
        self.background_image_tk = None
        self._load_background_image()

        # Styles
        self.style = ttk.Style()
        self.style.configure('Success.TLabel', foreground='#2ecc71')
        self.style.configure('Error.TLabel',   foreground='#e74c3c')
        self.style.configure('Warning.TLabel', foreground='#f39c12')

        # Redirect stdout → log widget
        self.original_stdout = None
        self._redirect_prints()

        # Build tabs
        self._create_widgets()

        # Status polling
        self.update_status()

        # Hotkey polling thread
        self._start_hotkey_thread()

    # ------------------------------------------------------------------
    # Background image
    # ------------------------------------------------------------------

    def _load_background_image(self):
        if not PIL_AVAILABLE:
            return
        try:
            bg_image = Image.open("background.png")
            self.background_image_tk = ImageTk.PhotoImage(bg_image)
            self.background_label = tk.Label(self.root, image=self.background_image_tk, borderwidth=0)
            self.background_label.place(x=0, y=0, relwidth=1, relheight=1)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"Warning: could not load background.png: {e}")

    # ------------------------------------------------------------------
    # Print redirect
    # ------------------------------------------------------------------

    def _redirect_prints(self):
        class _Redirect:
            def __init__(self, fn):
                self._fn = fn
            def write(self, msg):
                if msg.strip():
                    self._fn(msg.strip())
            def flush(self):
                pass

        self.original_stdout = sys.stdout
        sys.stdout = _Redirect(self.log)

    # ------------------------------------------------------------------
    # Widget creation
    # ------------------------------------------------------------------

    def _create_widgets(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=5, pady=2)

        self._create_main_tab()
        self._create_cockpit_tab()
        self._create_hotkeys_tab()
        self._create_tutorial_tab()

    # ---- Main tab -------------------------------------------------------

    def _create_main_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text='Main Control')

        # Title
        title_frame = ttk.Frame(tab)
        title_frame.pack(fill='x', padx=10, pady=10)
        ttk.Label(title_frame, text='echterbotv2 — TS3Bot Control Panel',
                  font=('Arial', 16, 'bold')).pack()

        # Main container (left + right)
        main_frame = ttk.Frame(tab)
        main_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # --- Left panel: connection & control ---
        left = ttk.LabelFrame(main_frame, text='Bot Control')
        left.pack(side='left', fill='both', expand=True, padx=(0, 8), ipadx=8, ipady=8)

        # Status row
        sf = ttk.Frame(left)
        sf.pack(fill='x', padx=8, pady=4)
        ttk.Label(sf, text='Status:').pack(side='left')
        self.status_label = ttk.Label(sf, text='Stopped', foreground='#e74c3c')
        self.status_label.pack(side='left', padx=(8, 0))

        # Connection parameters
        cfg_frame = ttk.LabelFrame(left, text='Connection Settings')
        cfg_frame.pack(fill='x', padx=8, pady=6)

        params = [
            ('Host',           'host',           self.config.get("host", "127.0.0.1:25639")),
            ('API Key',        'api_key',         self.config.get("api_key", "")),
            ('Server Address', 'server_address',  self.config.get("server_address", "")),
            ('Nickname',       'nickname',         self.config.get("nickname", "Rollabot")),
        ]
        self._cfg_vars: dict[str, tk.StringVar] = {}
        for display, key, default in params:
            row = ttk.Frame(cfg_frame)
            row.pack(fill='x', padx=6, pady=3)
            ttk.Label(row, text=f'{display}:', width=14, anchor='w').pack(side='left')
            var = tk.StringVar(value=default)
            show = '*' if key == 'api_key' else ''
            entry = ttk.Entry(row, textvariable=var, show=show, width=30)
            entry.pack(side='left', padx=(4, 0), fill='x', expand=True)
            self._cfg_vars[key] = var
            ToolTip(entry, f"Configure {display} for the TS3 ClientQuery connection.")

        # Debug toggle
        df = ttk.Frame(left)
        df.pack(fill='x', padx=8, pady=4)
        self.debug_var = tk.BooleanVar(value=self.config.get("debug", False))
        dchk = ttk.Checkbutton(df, text='Debug Mode', variable=self.debug_var,
                               command=self._on_debug_toggle)
        dchk.pack(side='left')
        ToolTip(dchk, "Enable verbose debug logging.")

        # Control buttons
        btn_frame = ttk.Frame(left)
        btn_frame.pack(fill='x', padx=8, pady=10)
        self.start_btn = ttk.Button(btn_frame, text='▶ Start Bot', command=self.start_bot)
        self.start_btn.pack(side='left', padx=(0, 6))
        self.stop_btn = ttk.Button(btn_frame, text='⏹ Stop Bot', command=self.stop_bot,
                                   state='disabled')
        self.stop_btn.pack(side='left', padx=6)
        self.reconnect_btn = ttk.Button(btn_frame, text='↺ Reconnect', command=self.reconnect_bot)
        self.reconnect_btn.pack(side='left', padx=6)
        ToolTip(self.start_btn,    "Start the TS3Bot event loop in a background thread.")
        ToolTip(self.stop_btn,     "Stop the TS3Bot event loop gracefully.")
        ToolTip(self.reconnect_btn,"Disconnect and reconnect to the TS3 server.")

        # Quick actions
        qa_frame = ttk.LabelFrame(left, text='Quick Actions')
        qa_frame.pack(fill='x', padx=8, pady=6)

        qa_btn_frame = ttk.Frame(qa_frame)
        qa_btn_frame.pack(fill='x', padx=4, pady=4)

        actions = [
            ('📢 Mass-poke',      self._action_masspoke),
            ('👥 Refresh Clients', self._action_refresh_clients),
            ('📊 War Stats',       self._action_war_stats),
        ]
        for label, cmd in actions:
            b = ttk.Button(qa_btn_frame, text=label, command=cmd, width=16)
            b.pack(side='left', padx=4, pady=2)

        # Masspoke message entry
        mp_row = ttk.Frame(qa_frame)
        mp_row.pack(fill='x', padx=6, pady=4)
        ttk.Label(mp_row, text='Poke message:', width=14, anchor='w').pack(side='left')
        self.masspoke_var = tk.StringVar(value="Attention! Bot online.")
        mp_entry = ttk.Entry(mp_row, textvariable=self.masspoke_var, width=28)
        mp_entry.pack(side='left', padx=(4, 0), fill='x', expand=True)
        ToolTip(mp_entry, "Message sent to all online clients when Mass-poke is triggered.")

        # --- Right panel: client list ---
        right = ttk.LabelFrame(main_frame, text='Online Clients')
        right.pack(side='right', fill='both', expand=True, padx=(8, 0), ipadx=6, ipady=6)

        cols = ('clid', 'Nickname', 'UID')
        self.client_tree = ttk.Treeview(right, columns=cols, show='headings', height=12)
        for c in cols:
            self.client_tree.heading(c, text=c)
            self.client_tree.column(c, width=90, anchor='w')
        cl_scroll = ttk.Scrollbar(right, orient='vertical', command=self.client_tree.yview)
        self.client_tree.configure(yscrollcommand=cl_scroll.set)
        self.client_tree.pack(side='left', fill='both', expand=True, padx=4, pady=4)
        cl_scroll.pack(side='right', fill='y', pady=4)

        refresh_frame = ttk.Frame(right)
        refresh_frame.pack(fill='x', padx=4, pady=2)
        ttk.Button(refresh_frame, text='Refresh',
                   command=self._action_refresh_clients).pack(side='left')

        # --- Log panel at bottom ---
        log_frame = ttk.LabelFrame(tab, text='Logs')
        log_frame.pack(fill='x', padx=10, pady=(0, 10), ipadx=4, ipady=4)

        self.log_text = tk.Text(log_frame, height=8, font=('Consolas', 9),
                                state='disabled', wrap=tk.WORD)
        log_scroll = ttk.Scrollbar(log_frame, orient='vertical', command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_scroll.set)
        self.log_text.pack(side='left', fill='both', expand=True, padx=4, pady=4)
        log_scroll.pack(side='right', fill='y', pady=4)

    # ---- Cockpit tab ----------------------------------------------------

    def _create_cockpit_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text='Cockpit')

        canvas = tk.Canvas(tab, highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab, orient='vertical', command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        canvas.pack(side='left', fill='both', expand=True)

        inner = ttk.Frame(scrollable_frame)
        inner.pack(expand=True, anchor='n')

        ttk.Label(inner, text='🖥 Bot Variables Cockpit',
                  font=('Arial', 14, 'bold')).pack(pady=12)

        panels = ttk.Frame(inner)
        panels.pack(fill='both', expand=True)

        left_panel  = ttk.Frame(panels, relief='groove', borderwidth=2)
        right_panel = ttk.Frame(panels, relief='groove', borderwidth=2)
        left_panel.pack(side='left',  fill='both', expand=True)
        right_panel.pack(side='right', fill='both', expand=True)

        # Connection section
        self._create_cockpit_section(left_panel, "Connection", [
            ('host',           'Host'),
            ('api_key_masked', 'API Key'),
            ('server_address', 'Server Address'),
            ('nickname',       'Nickname'),
        ])

        # Runtime section
        self._create_cockpit_section(left_panel, "Runtime", [
            ('bot_running',    'Bot Thread Active'),
            ('debug_mode',     'Debug Mode'),
            ('client_count',   'Online Clients'),
        ])

        # War Stats section
        self._create_cockpit_section(right_panel, "War Stats Cache", [
            ('war_stats_last_update', 'Last Updated'),
            ('war_stats_summary',     'Data'),
        ])

        # Command history section
        self._create_cockpit_section(right_panel, "Command History", [
            ('cmd_history_count', 'Commands Logged'),
        ])

        # Hotkey status
        self._create_cockpit_section(right_panel, "Hotkey Status", [
            ('hotkey_capslock', 'CapsLock Active'),
            ('hotkey_last',     'Last Hotkey'),
        ])

    def _create_cockpit_section(self, parent, title, variables):
        section = ttk.LabelFrame(parent, text=title)
        section.pack(fill='x', padx=12, pady=8, ipadx=8, ipady=8)
        for var_name, display_name in variables:
            row = ttk.Frame(section)
            row.pack(fill='x', padx=8, pady=3)
            ttk.Label(row, text=f'{display_name}:',
                      font=('Arial', 10), width=22, anchor='w').pack(side='left')
            val_label = ttk.Label(row, text='Loading...',
                                  font=('Arial', 10))
            val_label.pack(side='left', padx=(10, 0))
            setattr(self, f'cockpit_{var_name}_label', val_label)

    # ---- Hotkeys tab ----------------------------------------------------

    def _create_hotkeys_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text='Hotkeys')

        ttk.Label(tab, text='⌨ CapsLock + Key Hotkeys',
                  font=('Arial', 14, 'bold')).pack(pady=12)

        table_frame = ttk.Frame(tab)
        table_frame.pack(fill='both', expand=True, padx=20, pady=5)

        cols = ('Key', 'Action', 'Status')
        self.hotkey_tree = ttk.Treeview(table_frame, columns=cols,
                                        show='headings', height=10)
        for c in cols:
            self.hotkey_tree.heading(c, text=c)
        self.hotkey_tree.column('Key',    width=60,  anchor='center')
        self.hotkey_tree.column('Action', width=220, anchor='w')
        self.hotkey_tree.column('Status', width=120, anchor='center')

        for key, (desc, _) in self.HOTKEY_MAP.items():
            self.hotkey_tree.insert('', 'end', iid=key,
                                    values=(f'CapsLock + {key}', desc, '—'))
        self.hotkey_tree.pack(fill='both', expand=True)

        ttk.Label(tab,
                  text='Hotkeys are active only when CapsLock is ON.\n'
                       'Hold the corresponding key to trigger the action.',
                  font=('Arial', 9), foreground='gray').pack(pady=8)

    # ---- Tutorial tab ---------------------------------------------------

    def _create_tutorial_tab(self):
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text='Tutorial')

        ttk.Label(tab, text='📖 Setup & Configuration Guide',
                  font=('Arial', 14, 'bold')).pack(pady=12)

        frame = ttk.Frame(tab)
        frame.pack(fill='both', expand=True, padx=15, pady=5)

        txt = tk.Text(frame, wrap=tk.WORD, font=('Arial', 10),
                      state='disabled', padx=10, pady=10)
        scroll = ttk.Scrollbar(frame, orient='vertical', command=txt.yview)
        txt.configure(yscrollcommand=scroll.set)
        txt.pack(side='left', fill='both', expand=True)
        scroll.pack(side='right', fill='y')

        content = """\
echterbotv2 — Quick Start Guide
================================

1. ENVIRONMENT SETUP
   Create a .env file in the project root:

       TS3_HOST=127.0.0.1:25639
       TS3_API_KEY=your_api_key_here
       TS3_SERVER_ADDRESS=your.ts3.server.address:9987
       TS3_NICKNAME=Rollabot
       DEBUG=false

2. STARTING THE BOT
   • Fill in the Connection Settings on the Main Control tab.
   • Press '▶ Start Bot' or use CapsLock + W hotkey.
   • The bot will connect to the TS3 ClientQuery API.

3. HOTKEYS  (active only when CapsLock is ON)
   CapsLock + W  → Start bot
   CapsLock + S  → Stop bot
   CapsLock + A  → Reconnect
   CapsLock + D  → Disconnect
   CapsLock + E  → Show war stats in log
   CapsLock + Z  → Mass-poke all online users
   CapsLock + C  → Refresh client list
   CapsLock + Q  → Quit the application

4. QUICK ACTIONS
   • Mass-poke: sends the configured poke message to all online clients.
   • Refresh Clients: updates the Online Clients panel.
   • War Stats: fetches and prints the latest war statistics.

5. COCKPIT TAB
   Shows live values for connection params, thread status, war stats
   cache and hotkey state.

6. LOGS
   All output is captured in the Logs panel on the Main tab.
   Use Debug Mode for verbose output.
"""
        txt.config(state='normal')
        txt.insert(tk.END, content)
        txt.config(state='disabled')

    # ------------------------------------------------------------------
    # Bot control actions
    # ------------------------------------------------------------------

    def start_bot(self):
        """Start the TS3Bot in a background thread using current UI params."""
        if self._bot_thread and self._bot_thread.is_alive():
            self.log("Bot is already running.", level='WARNING')
            return
        try:
            # Apply UI config to bot
            self._apply_config_from_ui()
            self._running = True

            self._bot_thread = threading.Thread(
                target=self._bot_run_wrapper, daemon=True, name='TS3BotThread')
            self._bot_thread.start()

            self.start_btn.config(state='disabled')
            self.stop_btn.config(state='normal')
            self.status_label.config(text='Running', foreground='#2ecc71')
            self.log("Bot started.", level='SUCCESS')
        except Exception as e:
            self.log(f"Error starting bot: {e}", level='ERROR')
            self.status_label.config(text='Error', foreground='#e74c3c')

    def _bot_run_wrapper(self):
        try:
            self.bot.run()
        except Exception as e:
            self.log(f"Bot thread error: {e}", level='ERROR')
        finally:
            self._running = False
            try:
                self.root.after_idle(self._on_bot_stopped)
            except Exception:
                pass

    def _on_bot_stopped(self):
        self.start_btn.config(state='normal')
        self.stop_btn.config(state='disabled')
        self.status_label.config(text='Stopped', foreground='#e74c3c')
        self.log("Bot stopped.", level='INFO')

    def stop_bot(self):
        try:
            self.bot._running = False
            self.start_btn.config(state='normal')
            self.stop_btn.config(state='disabled')
            self.status_label.config(text='Stopped', foreground='#e74c3c')
            self.log("Stop signal sent to bot.", level='INFO')
        except Exception as e:
            self.log(f"Error stopping bot: {e}", level='ERROR')

    def reconnect_bot(self):
        try:
            self.stop_bot()
            time.sleep(0.5)
            self.log("Reconnecting...", level='INFO')
            self.start_bot()
        except Exception as e:
            self.log(f"Error reconnecting: {e}", level='ERROR')

    def _apply_config_from_ui(self):
        """Push UI field values into the bot instance."""
        self.bot.host           = self._cfg_vars['host'].get().strip()
        self.bot.api_key        = self._cfg_vars['api_key'].get().strip()
        self.bot.server_address = self._cfg_vars['server_address'].get().strip()
        self.bot.nickname       = self._cfg_vars['nickname'].get().strip()

    # ------------------------------------------------------------------
    # Quick action handlers
    # ------------------------------------------------------------------

    def _action_masspoke(self):
        msg = self.masspoke_var.get().strip()
        if not msg:
            self.log("Poke message is empty.", level='WARNING')
            return
        try:
            self.bot.masspoke(msg)
            self.log(f"Mass-poke sent: {msg}", level='SUCCESS')
        except Exception as e:
            self.log(f"Mass-poke error: {e}", level='ERROR')

    def _action_refresh_clients(self):
        try:
            self.client_tree.delete(*self.client_tree.get_children())
            if self.bot.client_map:
                for clid, info in self.bot.client_map.items():
                    nick = info.get('nickname', '') if isinstance(info, dict) else str(info)
                    uid  = info.get('uid', '') if isinstance(info, dict) else ''
                    self.client_tree.insert('', 'end', values=(clid, nick, uid))
            self.log(f"Client list refreshed ({len(self.bot.client_map)} clients).")
        except Exception as e:
            self.log(f"Error refreshing clients: {e}", level='ERROR')

    def _action_war_stats(self):
        try:
            stats, updated = self.bot.war_stats_collector.get_stats()
            if stats:
                self.log(f"War stats (updated {updated}): {stats}", level='INFO')
            else:
                self.log("War stats not available yet.", level='WARNING')
        except Exception as e:
            self.log(f"Error fetching war stats: {e}", level='ERROR')

    # ------------------------------------------------------------------
    # Hotkey actions (routed from hotkey thread via root.after_idle)
    # ------------------------------------------------------------------

    def hotkey_start(self):
        self.log("[Hotkey] Start bot triggered.")
        self.start_bot()

    def hotkey_stop(self):
        self.log("[Hotkey] Stop bot triggered.")
        self.stop_bot()

    def hotkey_reconnect(self):
        self.log("[Hotkey] Reconnect triggered.")
        self.reconnect_bot()

    def hotkey_disconnect(self):
        self.log("[Hotkey] Disconnect triggered.")
        self.stop_bot()

    def hotkey_war_stats(self):
        self.log("[Hotkey] War stats triggered.")
        self._action_war_stats()

    def hotkey_masspoke(self):
        self.log("[Hotkey] Mass-poke triggered.")
        self._action_masspoke()

    def hotkey_refresh_clients(self):
        self.log("[Hotkey] Refresh clients triggered.")
        self._action_refresh_clients()

    def hotkey_quit(self):
        self.log("[Hotkey] Quit triggered.")
        self.on_closing()

    # ------------------------------------------------------------------
    # Hotkey polling thread
    # ------------------------------------------------------------------

    def _start_hotkey_thread(self):
        self._hotkey_thread_running = True
        self._hotkey_thread = threading.Thread(
            target=self._hotkey_loop, daemon=True, name='HotkeyThread')
        self._hotkey_thread.start()

    def _hotkey_loop(self):
        """Poll for CapsLock + key combos and dispatch actions."""
        last_fired: str | None = None
        while self._hotkey_thread_running:
            try:
                if is_capslock_on():
                    key = get_pressed_hotkey()
                    if key and key != last_fired and key in self.HOTKEY_MAP:
                        last_fired = key
                        _, action_name = self.HOTKEY_MAP[key]
                        method = getattr(self, action_name, None)
                        if method:
                            self.root.after_idle(method)
                        # Update cockpit
                        self._last_hotkey_key = key
                        # Mark triggered in the hotkey table
                        self.root.after_idle(
                            lambda k=key: self.hotkey_tree.set(k, 'Status', '✓ Fired'))
                    elif not key:
                        last_fired = None
                        # Reset all statuses periodically
                        if last_fired is None:
                            pass
                else:
                    last_fired = None
            except Exception:
                pass
            time.sleep(0.05)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def log(self, message: str, level: str = 'INFO'):
        try:
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            formatted = f"[{ts}] [{level}] {message}"
            self.log_text.config(state='normal')
            self.log_text.insert(tk.END, formatted + "\n")
            self.log_text.see(tk.END)
            self.log_text.config(state='disabled')
            if level == 'ERROR':
                print(formatted, file=sys.__stderr__ if hasattr(sys, '__stderr__') else sys.stdout)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Misc callbacks
    # ------------------------------------------------------------------

    def _on_debug_toggle(self):
        level = logging.DEBUG if self.debug_var.get() else logging.INFO
        logging.getLogger().setLevel(level)
        self.log(f"Debug mode: {'ON' if self.debug_var.get() else 'OFF'}")

    # ------------------------------------------------------------------
    # Status / cockpit updates
    # ------------------------------------------------------------------

    def update_status(self):
        try:
            # Bot thread alive?
            running = self._bot_thread is not None and self._bot_thread.is_alive()
            if running:
                self.status_label.config(text='Running', foreground='#2ecc71')
                self.start_btn.config(state='disabled')
                self.stop_btn.config(state='normal')
            else:
                if self.status_label.cget('text') == 'Running':
                    self.status_label.config(text='Stopped', foreground='#e74c3c')
                    self.start_btn.config(state='normal')
                    self.stop_btn.config(state='disabled')

            self._update_cockit_values()
        except Exception as e:
            pass
        self.root.after(500, self.update_status)

    def _update_cockit_values(self):
        try:
            safe = lambda attr, default='N/A': str(getattr(self.bot, attr, default))
            def sl(name, val):
                label = getattr(self, f'cockpit_{name}_label', None)
                if label:
                    label.config(text=val)

            # Mask api key
            raw_key = getattr(self.bot, 'api_key', '')
            masked   = raw_key[:4] + '****' + raw_key[-4:] if len(raw_key) > 8 else '****'

            sl('host',           safe('host'))
            sl('api_key_masked', masked)
            sl('server_address', safe('server_address'))
            sl('nickname',       safe('nickname'))

            running = self._bot_thread is not None and self._bot_thread.is_alive()
            sl('bot_running', str(running))
            sl('debug_mode',  str(self.debug_var.get()))
            sl('client_count', str(len(getattr(self.bot, 'client_map', {}))))

            # War stats
            ws_cache, ws_ts = self.bot.war_stats_collector.get_stats()
            sl('war_stats_last_update',
               ws_ts.strftime('%H:%M:%S') if ws_ts else 'Never')
            sl('war_stats_summary',
               'Available' if ws_cache else 'No data')

            # Command history
            hist = getattr(self.bot, 'command_history', [])
            sl('cmd_history_count', str(len(hist)))

            # Hotkeys
            sl('hotkey_capslock', 'ON' if is_capslock_on() else 'off')
            sl('hotkey_last', self._last_hotkey_key or '—')

        except Exception:
            pass

    # ------------------------------------------------------------------
    # Window close
    # ------------------------------------------------------------------

    def on_closing(self):
        try:
            self._hotkey_thread_running = False
            self.bot._running = False
            self.log("Shutting down...", level='INFO')
        except Exception as e:
            print(f"Error on closing: {e}")
        finally:
            if self.original_stdout:
                sys.stdout = self.original_stdout
            self.root.destroy()

    def run(self):
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.root.mainloop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    print("=" * 60)
    print("echterbotv2 — TS3Bot Control Panel")
    print("=" * 60)

    try:
        config = load_config()
    except Exception as e:
        print(f"Warning: could not load .env config ({e}). Using defaults.")
        config = {
            "host": "127.0.0.1:25639",
            "api_key": "",
            "server_address": "",
            "nickname": "Rollabot",
            "debug": False,
            "client_command": "",
            "pid_file": ".tsclient.pid",
        }

    # Optionally set up TS client manager
    client_manager = None
    if config.get("client_command"):
        client_manager = TSClientManager(
            command=config["client_command"],
            pid_file=config["pid_file"]
        )

    # Create bot instance (not started yet — UI handles that)
    bot = TS3Bot(
        host=config["host"],
        api_key=config["api_key"],
        server_address=config["server_address"],
        nickname=config["nickname"],
        process_manager=client_manager,
    )

    print("Starting GUI...")
    gui = botUI(bot, config)
    gui.run()
