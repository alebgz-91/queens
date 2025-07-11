from utils import get_dukes_urls

# local paths of mapping templates
dukes_templates = {
    "chapter_1": "data/template/dukes_ch_1.xlsx"
}

# HTTP addresses of chapter webpages
dukes_chapter_urls = {
    "chapter_1": "https://www.gov.uk/government/statistics/energy-chapter-1-digest-of-united-kingdom-energy-statistics-dukes"
}

# scrape the links for all tables
dukes_tables_urls = {}

for chapter, url in dukes_chapter_urls.items():
    tb_urls = get_dukes_urls(url = url)
    dukes_tables_urls.update(tb_urls)

# mapping table to processing method - JSON style
dukes_config = {
    "chapter_1": {
        "template_file_path": dukes_templates["chapter_1"],
        "dukes_1_1": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_1"]["url"],
                "table_name": "1.1"
            }
        },

        "dukes_1_2": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_2"]["url"],
                "table_name": "1.2"
            }
        },

        "dukes_1_3": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_3"]["url"],
                "sheet_names": ["1.3.A", "1.3.B"]
            }
        },

        "dukes_1_1_1": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_1_1"]["url"],
                "sheet_names": ["1.1.1.A", "1.1.1.B", "1.1.1.C"]
            }
        },

        "dukes_1_1_2": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_1_2"]["url"],
                "sheet_names": ["1.1.2"],
                "map_on_cols": True
            }
        },

        "dukes_1_1_3": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_1_3"]["url"],
                "sheet_names": ["1.1.3"],
                "map_on_cols": True
            }
        },

        "dukes_1_1_4": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_1_4"]["url"],
                "sheet_names": ["1.1.4"],
                "map_on_cols": True
            }
        },

        "dukes_1_1_5": {
            "f": "process_dukes_1_1_5",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_1_5"]["url"],
            }
        },

        "dukes_1_1_6": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_1_1_6"]["url"],
                "sheet_names": ["1.1.6"],
                "map_on_cols": True
            }
        },

        "dukes_I_1": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_I_1"]["url"],
                "table_name": "I.1"
            }
        },

        "dukes_J_1": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": dukes_tables_urls["dukes_J_1"]["url"],
                "table_name": "J.1"
            }
        },
    }
}

