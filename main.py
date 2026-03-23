import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from bitrix_importer import (
    ImportConfig,
    default_result_file,
    format_deal_metadata,
    format_lead_statuses,
    get_deal_categories,
    get_deal_stages,
    get_lead_statuses,
    import_file,
    load_excel_preview,
    load_webhook_from_env,
    save_webhook_to_env,
)


class BitrixImporterApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Bitrix Importer")
        self.geometry("1180x760")
        self.minsize(980, 680)

        self.deal_categories = []
        self.deal_stages = {}
        self.lead_statuses = []
        self.worker: threading.Thread | None = None

        self.webhook_var = tk.StringVar(value=load_webhook_from_env())
        self.file_var = tk.StringVar()
        self.result_var = tk.StringVar()
        self.entity_type_var = tk.StringVar(value="deal")
        self.contact_mode_var = tk.StringVar(value="entities")
        self.deal_category_var = tk.StringVar()
        self.deal_stage_var = tk.StringVar()
        self.lead_status_var = tk.StringVar(value="NEW")
        self.progress_var = tk.StringVar(value="Готово к работе")
        self._context_menu: tk.Menu | None = None

        self._build_ui()
        self._toggle_target_fields()
        self._bind_text_actions()

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=12)
        root.pack(fill="both", expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(3, weight=1)

        settings = ttk.LabelFrame(root, text="Настройки", padding=12)
        settings.grid(row=0, column=0, sticky="ew")
        settings.columnconfigure(1, weight=1)
        settings.columnconfigure(3, weight=1)

        ttk.Label(settings, text="Webhook").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.webhook_entry = ttk.Entry(settings, textvariable=self.webhook_var)
        self.webhook_entry.grid(row=0, column=1, columnspan=3, sticky="ew", pady=4)
        ttk.Button(settings, text="Сохранить webhook", command=self._save_webhook).grid(
            row=0, column=4, sticky="ew", padx=(8, 0), pady=4
        )

        ttk.Label(settings, text="Куда загружать").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        entity_combo = ttk.Combobox(
            settings,
            textvariable=self.entity_type_var,
            state="readonly",
            values=["deal", "lead"],
        )
        entity_combo.grid(row=1, column=1, sticky="ew", pady=4)
        entity_combo.bind("<<ComboboxSelected>>", lambda _event: self._toggle_target_fields())

        ttk.Label(settings, text="Контакты").grid(row=1, column=2, sticky="w", padx=(12, 8), pady=4)
        ttk.Combobox(
            settings,
            textvariable=self.contact_mode_var,
            state="readonly",
            values=["entities", "comments"],
        ).grid(row=1, column=3, sticky="ew", pady=4)

        ttk.Button(settings, text="Показать лиды", command=self._load_leads_metadata).grid(
            row=1, column=4, sticky="ew", padx=(8, 0), pady=4
        )

        ttk.Label(settings, text="Воронка сделки").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        self.deal_category_combo = ttk.Combobox(settings, textvariable=self.deal_category_var, state="readonly")
        self.deal_category_combo.grid(row=2, column=1, sticky="ew", pady=4)
        self.deal_category_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_category_change())

        ttk.Label(settings, text="Стадия сделки").grid(row=2, column=2, sticky="w", padx=(12, 8), pady=4)
        self.deal_stage_combo = ttk.Combobox(settings, textvariable=self.deal_stage_var, state="readonly")
        self.deal_stage_combo.grid(row=2, column=3, sticky="ew", pady=4)

        ttk.Button(settings, text="Показать сделки", command=self._load_deals_metadata).grid(
            row=2, column=4, sticky="ew", padx=(8, 0), pady=4
        )

        ttk.Label(settings, text="Статус лида").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        self.lead_status_combo = ttk.Combobox(settings, textvariable=self.lead_status_var, state="readonly")
        self.lead_status_combo.grid(row=3, column=1, sticky="ew", pady=4)

        file_box = ttk.LabelFrame(root, text="Файл", padding=12)
        file_box.grid(row=1, column=0, sticky="ew", pady=(12, 0))
        file_box.columnconfigure(1, weight=1)

        ttk.Label(file_box, text="Excel файл").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.file_entry = ttk.Entry(file_box, textvariable=self.file_var)
        self.file_entry.grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Button(file_box, text="Выбрать файл", command=self._select_file).grid(row=0, column=2, padx=(8, 0), pady=4)

        ttk.Label(file_box, text="Отчет").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.result_entry = ttk.Entry(file_box, textvariable=self.result_var)
        self.result_entry.grid(row=1, column=1, sticky="ew", pady=4)
        ttk.Button(file_box, text="Просмотреть файл", command=self._preview_file).grid(
            row=1, column=2, padx=(8, 0), pady=4
        )

        actions = ttk.Frame(root)
        actions.grid(row=2, column=0, sticky="ew", pady=(12, 0))
        actions.columnconfigure(0, weight=1)

        ttk.Label(actions, textvariable=self.progress_var).grid(row=0, column=0, sticky="w")
        self.import_button = ttk.Button(actions, text="Запустить импорт", command=self._start_import)
        self.import_button.grid(row=0, column=1, sticky="e")

        log_box = ttk.LabelFrame(root, text="Журнал", padding=12)
        log_box.grid(row=3, column=0, sticky="nsew", pady=(12, 0))
        log_box.columnconfigure(0, weight=1)
        log_box.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_box, wrap="word")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(log_box, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

        self._log("Режимы контактов:")
        self._log("entities = создавать компанию и контакт в CRM")
        self._log("comments = не создавать сущности, писать контакты в комментарий")

    def _bind_text_actions(self) -> None:
        widgets = [
            self.webhook_entry,
            self.file_entry,
            self.result_entry,
            self.log_text,
            self.deal_category_combo,
            self.deal_stage_combo,
            self.lead_status_combo,
        ]
        for widget in widgets:
            widget.bind("<Button-3>", self._show_context_menu)
            widget.bind("<Control-v>", self._paste_into_widget)
            widget.bind("<Control-V>", self._paste_into_widget)
            widget.bind("<Shift-Insert>", self._paste_into_widget)

        self.webhook_entry.focus_set()

    def _show_context_menu(self, event: tk.Event) -> str:
        widget = event.widget
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="Вырезать", command=lambda: self._event_generate(widget, "<<Cut>>"))
        menu.add_command(label="Копировать", command=lambda: self._event_generate(widget, "<<Copy>>"))
        menu.add_command(label="Вставить", command=lambda: self._event_generate(widget, "<<Paste>>"))
        if widget is self.log_text:
            menu.add_separator()
            menu.add_command(label="Выделить все", command=lambda: self._select_all_text(widget))
        self._context_menu = menu
        menu.tk_popup(event.x_root, event.y_root)
        return "break"

    def _event_generate(self, widget: tk.Widget, virtual_event: str) -> None:
        widget.focus_force()
        widget.event_generate(virtual_event)

    def _paste_into_widget(self, event: tk.Event) -> str:
        widget = event.widget
        widget.focus_force()
        widget.event_generate("<<Paste>>")
        return "break"

    def _select_all_text(self, widget: tk.Widget) -> None:
        if widget is self.log_text:
            widget.tag_add("sel", "1.0", "end")
            widget.mark_set("insert", "1.0")

    def _log(self, message: str) -> None:
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.update_idletasks()

    def _save_webhook(self) -> None:
        webhook = self.webhook_var.get().strip()
        if not webhook:
            messagebox.showerror("Ошибка", "Введите webhook.")
            return
        save_webhook_to_env(webhook)
        self._log("Webhook сохранен в .env")

    def _select_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Выберите Excel файл",
            filetypes=[("Excel files", "*.xlsx *.xls")],
        )
        if not path:
            return
        self.file_var.set(path)
        self.result_var.set(default_result_file(path, self.entity_type_var.get()))

    def _preview_file(self) -> None:
        path = self.file_var.get().strip()
        if not path:
            messagebox.showerror("Ошибка", "Сначала выберите файл.")
            return
        try:
            preview = load_excel_preview(path)
        except Exception as exc:
            messagebox.showerror("Ошибка", str(exc))
            return

        self._log("")
        self._log("=== ПРЕДПРОСМОТР ФАЙЛА ===")
        self._log("Колонки:")
        for column in preview.columns:
            self._log(f"- {column}")
        self._log("")
        self._log(preview.fillna("").to_string(index=False))

    def _toggle_target_fields(self) -> None:
        entity_type = self.entity_type_var.get()
        self.result_var.set(default_result_file(self.file_var.get() or "import.xlsx", entity_type))

        if entity_type == "deal":
            self.deal_category_combo.configure(state="readonly")
            self.deal_stage_combo.configure(state="readonly")
            self.lead_status_combo.configure(state="disabled")
        else:
            self.deal_category_combo.configure(state="disabled")
            self.deal_stage_combo.configure(state="disabled")
            self.lead_status_combo.configure(state="readonly")

    def _load_deals_metadata(self) -> None:
        webhook = self.webhook_var.get().strip()
        if not webhook:
            messagebox.showerror("Ошибка", "Введите webhook.")
            return
        self._save_webhook()
        self.progress_var.set("Загружаю воронки и стадии сделок...")

        def worker() -> None:
            try:
                categories = get_deal_categories(webhook)
                stages = get_deal_stages(webhook)
                self.after(0, lambda: self._apply_deal_metadata(categories, stages))
            except Exception as exc:
                self.after(0, lambda: messagebox.showerror("Ошибка", str(exc)))
                self.after(0, lambda: self.progress_var.set("Ошибка загрузки сделок"))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_deal_metadata(self, categories, stages) -> None:
        self.deal_categories = categories
        self.deal_stages = stages

        category_values = [f"{item.get('id')} | {item.get('name')}" for item in categories]
        self.deal_category_combo["values"] = category_values
        if category_values and not self.deal_category_var.get():
            self.deal_category_var.set(category_values[0])
            self._on_category_change()

        self._log("")
        self._log(format_deal_metadata(categories, stages))
        self.progress_var.set("Воронки сделок загружены")
        self._toggle_target_fields()

    def _on_category_change(self) -> None:
        raw = self.deal_category_var.get().split("|", 1)[0].strip()
        stages = self.deal_stages.get(raw, [])
        stage_values = [f"{item.get('STATUS_ID')} | {item.get('NAME')}" for item in stages]
        self.deal_stage_combo["values"] = stage_values
        if stage_values:
            self.deal_stage_var.set(stage_values[0])
        else:
            self.deal_stage_var.set("")

    def _load_leads_metadata(self) -> None:
        webhook = self.webhook_var.get().strip()
        if not webhook:
            messagebox.showerror("Ошибка", "Введите webhook.")
            return
        self._save_webhook()
        self.progress_var.set("Загружаю статусы лидов...")

        def worker() -> None:
            try:
                statuses = get_lead_statuses(webhook)
                self.after(0, lambda: self._apply_lead_metadata(statuses))
            except Exception as exc:
                self.after(0, lambda: messagebox.showerror("Ошибка", str(exc)))
                self.after(0, lambda: self.progress_var.set("Ошибка загрузки лидов"))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_lead_metadata(self, statuses) -> None:
        self.lead_statuses = statuses
        values = [f"{item.get('STATUS_ID')} | {item.get('NAME')}" for item in statuses]
        self.lead_status_combo["values"] = values
        if values and not self.lead_status_var.get():
            self.lead_status_var.set(values[0])
        self._log("")
        self._log(format_lead_statuses(statuses))
        self.progress_var.set("Статусы лидов загружены")
        self._toggle_target_fields()

    def _start_import(self) -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Импорт", "Импорт уже выполняется.")
            return

        webhook = self.webhook_var.get().strip()
        excel_file = self.file_var.get().strip()
        if not webhook:
            messagebox.showerror("Ошибка", "Введите webhook.")
            return
        if not excel_file:
            messagebox.showerror("Ошибка", "Выберите Excel файл.")
            return
        if not Path(excel_file).exists():
            messagebox.showerror("Ошибка", "Файл не найден.")
            return

        entity_type = self.entity_type_var.get()
        config = ImportConfig(
            webhook_url=webhook,
            excel_file=excel_file,
            entity_type=entity_type,
            contact_mode=self.contact_mode_var.get(),
            result_file=self.result_var.get().strip() or None,
            deal_category_id=self._selected_category_id(),
            deal_stage_id=self._selected_stage_id(),
            lead_status_id=self._selected_lead_status_id(),
        )

        if entity_type == "deal":
            if config.deal_category_id is None:
                messagebox.showerror("Ошибка", "Выберите воронку сделки.")
                return
            if not config.deal_stage_id:
                messagebox.showerror("Ошибка", "Выберите стадию сделки.")
                return
        else:
            if not config.lead_status_id:
                messagebox.showerror("Ошибка", "Выберите статус лида.")
                return

        self._save_webhook()
        self.import_button.configure(state="disabled")
        self.progress_var.set("Импорт выполняется...")
        self._log("")
        self._log("=== СТАРТ ИМПОРТА ===")
        self._log(f"Тип: {entity_type}")
        self._log(f"Файл: {excel_file}")
        self._log(f"Контакты: {self.contact_mode_var.get()}")

        def worker() -> None:
            try:
                summary = import_file(
                    config,
                    log=lambda text: self.after(0, lambda value=text: self._log(value)),
                    progress=lambda current, total: self.after(
                        0, lambda: self.progress_var.set(f"Импорт: {current}/{total}")
                    ),
                )
                self.after(0, lambda: self._finish_import(summary))
            except Exception as exc:
                self.after(0, lambda: self._fail_import(exc))

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def _finish_import(self, summary) -> None:
        self.import_button.configure(state="normal")
        self.progress_var.set(
            f"Готово: успешно {summary['ok_count']}, ошибок {summary['error_count']}"
        )
        self.result_var.set(summary["result_file"])
        messagebox.showinfo(
            "Импорт завершен",
            f"Успешно: {summary['ok_count']}\nОшибок: {summary['error_count']}\nОтчет: {summary['result_file']}",
        )

    def _fail_import(self, exc: Exception) -> None:
        self.import_button.configure(state="normal")
        self.progress_var.set("Ошибка импорта")
        self._log(f"[FATAL] {exc}")
        messagebox.showerror("Ошибка импорта", str(exc))

    def _selected_category_id(self) -> int | None:
        value = self.deal_category_var.get().split("|", 1)[0].strip()
        if not value:
            return None
        return int(value)

    def _selected_stage_id(self) -> str:
        return self.deal_stage_var.get().split("|", 1)[0].strip()

    def _selected_lead_status_id(self) -> str:
        return self.lead_status_var.get().split("|", 1)[0].strip()


if __name__ == "__main__":
    app = BitrixImporterApp()
    app.mainloop()
