# -----------------------------------------------
# This file contains static configuration JSONs
# and dictionaries that redirect to the correct
# configuration JSON for each item.
# These should be treated as environment vars.
# ---------------------------------------------

# local paths of mapping templates
DUKES_TEMPLATES = {
    "chapter_1": "data/templates/dukes_ch_1.xlsx"
}

# HTTP addresses of chapter webpages
DUKES_CHAPTERS_URLS = {
    "chapter_1": "https://www.gov.uk/government/statistics/energy-chapter-1-digest-of-united-kingdom-energy-statistics-dukes"
}


# static mappings dict for processing methods - JSON style
DUKES_CONFIG = {
    "chapter_1": {
        "dukes_1_1": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": "",
                "table_name": "1.1"
            }
        },

        "dukes_1_2": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": "",
                "table_name": "1.2"
            }
        },

        "dukes_1_3": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": "",
                "sheet_names": ["1.3.A", "1.3.B"]
            }
        },

        "dukes_1_1_1": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": "",
                "sheet_names": ["1.1.1.A", "1.1.1.B", "1.1.1.C"]
            }
        },

        "dukes_1_1_2": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": "",
                "sheet_names": ["1.1.2"],
                "map_on_cols": True
            }
        },

        "dukes_1_1_3": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": "",
                "sheet_names": ["1.1.3"],
                "map_on_cols": True
            }
        },

        "dukes_1_1_4": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": "",
                "sheet_names": ["1.1.4"],
                "map_on_cols": True
            }
        },

        "dukes_1_1_5": {
            "f": "process_dukes_1_1_5",
            "f_args": {
                "url": "",
            }
        },

        "dukes_1_1_6": {
            "f": "process_sheet_to_frame",
            "f_args": {
                "url": "",
                "sheet_names": ["1.1.6"],
                "map_on_cols": True
            }
        },

        "dukes_I_1": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": "",
                "table_name": "I.1"
            }
        },

        "dukes_J_1": {
            "f": "process_multi_sheets_to_frame",
            "f_args": {
                "url": "",
                "table_name": "J.1"
            }
        }
        }

}


# dict of data collection configs
configs_dict = {
    "dukes": DUKES_CONFIG
}

# data collection urls dicts
urls_dictionaries = {
    "dukes": DUKES_CHAPTERS_URLS
}

# template urls
templates_dicts = {
    "dukes": DUKES_TEMPLATES
}