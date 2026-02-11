import sqlite3
from pathlib import Path
from typing import List, Tuple

from prep_brain.config import get_db_path as _configured_db_path
from prep_brain.config import load_config

CONFIG = load_config()


DB_PATH = _configured_db_path()


def get_db_path() -> Path:
    return DB_PATH

def get_conn() -> sqlite3.Connection:
    """Get a database connection."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def _conn() -> sqlite3.Connection:
    return get_conn()

def init_db() -> None:
    con = _conn()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        telegram_user_id INTEGER UNIQUE,
        display_name TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY,
        telegram_chat_id INTEGER,
        telegram_user_id INTEGER,
        title TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY,
        session_id INTEGER,
        role TEXT CHECK(role IN ('user','assistant','system')) NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    );
    """)

    # Kitchen Operations Tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory_items (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        quantity REAL DEFAULT 0,
        unit TEXT DEFAULT 'unit',
        cost REAL DEFAULT 0.0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS waste_logs (
        id INTEGER PRIMARY KEY,
        item_name TEXT NOT NULL,
        quantity_lost REAL NOT NULL,
        reason TEXT,
        logged_by TEXT,
        logged_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS prep_tasks (
        id INTEGER PRIMARY KEY,
        task TEXT NOT NULL,
        status TEXT DEFAULT 'todo' CHECK(status IN ('todo', 'done')),
        assigned_to TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shopping_list (
        id INTEGER PRIMARY KEY,
        item_name TEXT NOT NULL,
        quantity REAL NOT NULL,
        unit TEXT DEFAULT 'unit',
        added_by TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Vendors / Providers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vendors (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        contact_name TEXT,
        email TEXT,
        phone TEXT,
        website TEXT,
        ordering_method TEXT, -- email, text, portal, call
        cutoff_time TEXT,
        lead_time_days INTEGER DEFAULT 1,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Order Guide Items
    cur.execute("""
    CREATE TABLE IF NOT EXISTS order_guide_items (
        id INTEGER PRIMARY KEY,
        vendor_id INTEGER NOT NULL,
        item_name TEXT NOT NULL,
        pack_size TEXT,
        price REAL DEFAULT 0.0,
        par_level REAL DEFAULT 0.0,
        category TEXT,
        notes TEXT,
        is_active INTEGER DEFAULT 1,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(vendor_id) REFERENCES vendors(id) ON DELETE CASCADE,
        UNIQUE(vendor_id, item_name)
    );
    """)

    # Vendor catalog items (dashboard/API compatibility + invoice matching)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vendor_items (
        id INTEGER PRIMARY KEY,
        vendor_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        item_code TEXT,
        unit TEXT,
        price REAL,
        category TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(vendor_id) REFERENCES vendors(id) ON DELETE CASCADE
    );
    """)

    # Storage Areas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS storage_areas (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        sort_order INTEGER DEFAULT 0
    );
    """)

    # Inventory Counts History
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inventory_counts (
        id INTEGER PRIMARY KEY,
        item_name TEXT NOT NULL,
        quantity REAL NOT NULL,
        previous_quantity REAL,
        variance REAL,
        counted_by TEXT,
        counted_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Recipes
    cur.execute("""
    CREATE TABLE IF NOT EXISTS recipes (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        yield_amount REAL,
        yield_unit TEXT,
        station TEXT,
        category TEXT,
        method TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Recipe Ingredients
    cur.execute("""
    CREATE TABLE IF NOT EXISTS recipe_ingredients (
        id INTEGER PRIMARY KEY,
        recipe_id INTEGER NOT NULL,
        inventory_item_id INTEGER, -- Link to inventory if possible
        item_name_text TEXT,       -- Fallback if not linked
        quantity REAL,
        unit TEXT,
        canonical_value REAL,
        canonical_unit TEXT,
        display_original TEXT,
        display_pretty TEXT,
        cost REAL,                 -- Calculated cost for this amount (snapshot)
        notes TEXT,
        FOREIGN KEY(recipe_id) REFERENCES recipes(id) ON DELETE CASCADE,
        FOREIGN KEY(inventory_item_id) REFERENCES inventory_items(id)
    );
    """)

    # Menu Items
    cur.execute("""
    CREATE TABLE IF NOT EXISTS menu_items (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        recipe_id INTEGER, -- Link to recipe for cost
        selling_price REAL DEFAULT 0.0,
        category TEXT, -- Appetizer, Entree, etc.
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(recipe_id) REFERENCES recipes(id)
    );
    """)

    # Sales Log
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sales_log (
        id INTEGER PRIMARY KEY,
        menu_item_id INTEGER NOT NULL,
        quantity_sold INTEGER DEFAULT 0,
        date_logged TEXT DEFAULT CURRENT_DATE, -- YYYY-MM-DD
        logged_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(menu_item_id) REFERENCES menu_items(id)
    );
    """)

    # Ensure inventory_items has new columns (Migration logic)
    # We do this by checking if columns exist, if not, adding them.
    # SQLite `PRAGMA table_info` gives us columns.
    cur.execute("PRAGMA table_info(inventory_items)")
    columns = [row[1] for row in cur.fetchall()]
    
    if "storage_area_id" not in columns:
        cur.execute("ALTER TABLE inventory_items ADD COLUMN storage_area_id INTEGER REFERENCES storage_areas(id)")
    if "category" not in columns:
        cur.execute("ALTER TABLE inventory_items ADD COLUMN category TEXT")
    if "sort_order" not in columns:
        cur.execute("ALTER TABLE inventory_items ADD COLUMN sort_order INTEGER DEFAULT 0")

    # Prep List Items (Auto-generated)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS prep_list_items (
        id INTEGER PRIMARY KEY,
        recipe_id INTEGER NOT NULL,
        need_quantity REAL NOT NULL,
        unit TEXT,
        status TEXT DEFAULT 'todo', -- todo, done
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(recipe_id) REFERENCES recipes(id)
    );
    """)

    # Canonical stations registry for station-first prep display.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stations (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        description TEXT,
        instructions TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Ensure inventory_items has new columns (Migration logic)
    # ... (existing migrations)

    # Allergens
    cur.execute("""
    CREATE TABLE IF NOT EXISTS allergens (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );
    """)

    # Recipe Allergens (many-to-many)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS recipe_allergens (
        id INTEGER PRIMARY KEY,
        recipe_id INTEGER NOT NULL,
        allergen_id INTEGER NOT NULL,
        FOREIGN KEY(recipe_id) REFERENCES recipes(id) ON DELETE CASCADE,
        FOREIGN KEY(allergen_id) REFERENCES allergens(id),
        UNIQUE(recipe_id, allergen_id)
    );
    """)

    # Pre-populate allergens (Big 9)
    big_9 = ["Milk", "Eggs", "Fish", "Shellfish", "Tree Nuts", "Peanuts", "Wheat", "Soybeans", "Sesame"]
    for a in big_9:
        cur.execute("INSERT OR IGNORE INTO allergens (name) VALUES (?)", (a,))

    # Receiving Log
    cur.execute("""
    CREATE TABLE IF NOT EXISTS receiving_log (
        id INTEGER PRIMARY KEY,
        vendor_id INTEGER,
        invoice_number TEXT,
        item_name TEXT NOT NULL,
        quantity_received REAL,
        unit TEXT,
        unit_cost REAL,
        total_cost REAL,
        temperature_check REAL, -- °F for cold items
        quality_ok INTEGER DEFAULT 1, -- 1=Pass, 0=Reject
        notes TEXT,
        received_by TEXT,
        received_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(vendor_id) REFERENCES vendors(id)
    );
    """)

    # Waste Log (enhanced with category)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS waste_tracking (
        id INTEGER PRIMARY KEY,
        item_name TEXT NOT NULL,
        quantity REAL NOT NULL,
        unit TEXT,
        reason TEXT, -- Expired, Spoiled, Overproduction, Dropped, Burnt
        category TEXT, -- Raw, Prepped, Finished
        dollar_value REAL DEFAULT 0.0,
        logged_by TEXT,
        logged_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Staff
    cur.execute("""
    CREATE TABLE IF NOT EXISTS staff (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT, -- Chef, Sous, Prep, Line
        telegram_chat_id INTEGER,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Production Assignments
    cur.execute("""
    CREATE TABLE IF NOT EXISTS production_assignments (
        id INTEGER PRIMARY KEY,
        prep_list_item_id INTEGER NOT NULL,
        staff_id INTEGER NOT NULL,
        assigned_date TEXT DEFAULT CURRENT_DATE, -- YYYY-MM-DD
        shift TEXT, -- AM, PM, All
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(prep_list_item_id) REFERENCES prep_list_items(id) ON DELETE CASCADE,
        FOREIGN KEY(staff_id) REFERENCES staff(id)
    );
    """)

    # Ensure inventory_items has new columns (Migration logic)
    # ... (existing migrations)

    # Ensure recipe_ingredients has cost column (Migration)
    cur.execute("PRAGMA table_info(recipe_ingredients)")
    r_columns = [row[1] for row in cur.fetchall()]
    if "cost" not in r_columns:
        cur.execute("ALTER TABLE recipe_ingredients ADD COLUMN cost REAL")
    if "canonical_value" not in r_columns:
        cur.execute("ALTER TABLE recipe_ingredients ADD COLUMN canonical_value REAL")
    if "canonical_unit" not in r_columns:
        cur.execute("ALTER TABLE recipe_ingredients ADD COLUMN canonical_unit TEXT")
    if "display_original" not in r_columns:
        cur.execute("ALTER TABLE recipe_ingredients ADD COLUMN display_original TEXT")
    if "display_pretty" not in r_columns:
        cur.execute("ALTER TABLE recipe_ingredients ADD COLUMN display_pretty TEXT")

    # Ensure recipes has new columns (Migration)
    cur.execute("PRAGMA table_info(recipes)")
    recipe_cols = [row[1] for row in cur.fetchall()]
    if "par_level" not in recipe_cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN par_level REAL DEFAULT 0.0")
    if "output_inventory_item_id" not in recipe_cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN output_inventory_item_id INTEGER REFERENCES inventory_items(id)")
    if "sales_price" not in recipe_cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN sales_price REAL DEFAULT 0")
    if "recent_sales_count" not in recipe_cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN recent_sales_count INTEGER DEFAULT 0")
    if "on_hand" not in recipe_cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN on_hand REAL DEFAULT 0")
    if "unit" not in recipe_cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN unit TEXT")
    if "updated_at" not in recipe_cols:
        cur.execute("ALTER TABLE recipes ADD COLUMN updated_at TEXT")

    # 86 Board
    cur.execute("""
    CREATE TABLE IF NOT EXISTS eighty_six_board (
        id INTEGER PRIMARY KEY,
        item_name TEXT NOT NULL,
        reason TEXT, -- Out of stock, Quality, 86'd by Chef
        substitution TEXT, -- suggested sub
        reported_by TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        resolved_at TEXT
    );
    """)

    # Service Notes
    cur.execute("""
    CREATE TABLE IF NOT EXISTS service_notes (
        id INTEGER PRIMARY KEY,
        service_date TEXT DEFAULT CURRENT_DATE,
        shift TEXT, -- Lunch, Dinner, Brunch
        covers INTEGER DEFAULT 0,
        weather TEXT,
        notes TEXT, -- free-form markdown
        highlights TEXT, -- what went well
        issues TEXT, -- what went wrong
        action_items TEXT, -- follow-up tasks
        logged_by TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Chef Questions Test Suite
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chef_questions (
        id INTEGER PRIMARY KEY,
        question TEXT NOT NULL,
        expected_answer TEXT,
        category TEXT, -- Inventory, Recipes, Operations, Vendors
        last_tested_at TEXT,
        last_result TEXT, -- Pass, Fail, Partial
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Recipe Drafts (Autonomy: extracted recipes pending review/promotion)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS recipe_drafts (
        id INTEGER PRIMARY KEY,
        source_id TEXT,
        name TEXT NOT NULL,
        raw_text TEXT,
        yield_amount REAL,
        yield_unit TEXT,
        station TEXT,
        category TEXT,
        method TEXT,
        ingredients_json TEXT,
        allergens_json TEXT,
        confidence REAL DEFAULT 0.0,
        status TEXT DEFAULT 'pending' CHECK(status IN ('pending','enriched','promoted','rejected')),
        rejection_reason TEXT,
        knowledge_tier TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Autonomy Log (tracks all autonomous actions)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS autonomy_log (
        id INTEGER PRIMARY KEY,
        action TEXT NOT NULL,
        target_type TEXT,
        target_id TEXT,
        detail TEXT,
        confidence_before REAL,
        confidence_after REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Operational audit trail for deterministic write actions.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit_events (
        id INTEGER PRIMARY KEY,
        actor_telegram_user_id INTEGER,
        actor_display_name TEXT,
        action_type TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id INTEGER NOT NULL,
        old_value TEXT,
        new_value TEXT,
        note TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_entity_time ON audit_events(entity_type, entity_id, created_at)")

    # Autonomy heartbeat/state (single-row table; id=1).
    cur.execute("""
    CREATE TABLE IF NOT EXISTS autonomy_status (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        is_running INTEGER DEFAULT 0,
        last_tick_at TEXT,
        last_cycle_started_at TEXT,
        last_cycle_finished_at TEXT,
        last_action TEXT,
        last_error TEXT,
        last_error_at TEXT,
        queue_pending_drafts INTEGER DEFAULT 0,
        queue_pending_ingests INTEGER DEFAULT 0,
        last_promoted_recipe_id INTEGER,
        last_promoted_recipe_name TEXT,
        last_promoted_at TEXT
    );
    """)
    cur.execute("INSERT OR IGNORE INTO autonomy_status (id, is_running) VALUES (1, 0)")

    # Ingest jobs (document ingestion lifecycle + progress).
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ingest_jobs (
        id INTEGER PRIMARY KEY,
        ingest_id TEXT UNIQUE NOT NULL,
        source_filename TEXT NOT NULL,
        source_type TEXT DEFAULT 'unknown' CHECK(source_type IN ('restaurant_recipes','general_knowledge','unknown')),
        restaurant_tag TEXT,
        status TEXT DEFAULT 'queued' CHECK(status IN (
            'queued','extracting','chunking','indexing','extracting_recipes','enriching','promoting',
            'done','failed','needs_review'
        )),
        progress_current INTEGER DEFAULT 0,
        progress_total INTEGER DEFAULT 0,
        started_at TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        finished_at TEXT,
        error TEXT,
        promoted_count INTEGER DEFAULT 0,
        needs_review_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ingest_jobs_status_updated ON ingest_jobs(status, updated_at)")

    # Web-derived pricing estimates (non-authoritative)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS price_estimates (
        id INTEGER PRIMARY KEY,
        item_name TEXT NOT NULL,
        low_price REAL,
        high_price REAL,
        unit TEXT,
        source_urls TEXT,
        knowledge_tier TEXT DEFAULT 'general_knowledge_web',
        retrieved_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_price_estimates_item_time ON price_estimates(item_name, retrieved_at)")

    # Migration safety for older schema revisions.
    cur.execute("PRAGMA table_info(price_estimates)")
    pe_columns = [row[1] for row in cur.fetchall()]
    if pe_columns and "knowledge_tier" not in pe_columns:
        cur.execute("ALTER TABLE price_estimates ADD COLUMN knowledge_tier TEXT DEFAULT 'general_knowledge_web'")
    if pe_columns and "retrieved_at" not in pe_columns:
        cur.execute("ALTER TABLE price_estimates ADD COLUMN retrieved_at TEXT")
    if pe_columns:
        cur.execute(
            "UPDATE price_estimates SET retrieved_at = COALESCE(retrieved_at, CURRENT_TIMESTAMP) WHERE retrieved_at IS NULL"
        )

    # Ingest audit table for deterministic troubleshooting.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS doc_sources (
        id INTEGER PRIMARY KEY,
        ingest_id TEXT UNIQUE NOT NULL,
        filename TEXT NOT NULL,
        source_type TEXT DEFAULT 'unknown' CHECK(source_type IN ('restaurant_recipes','general_knowledge','general_knowledge_web','unknown')),
        restaurant_tag TEXT,
        file_sha256 TEXT,
        file_size INTEGER DEFAULT 0,
        extracted_text_chars INTEGER DEFAULT 0,
        chunk_count INTEGER DEFAULT 0,
        chunks_added INTEGER DEFAULT 0,
        status TEXT DEFAULT 'queued' CHECK(status IN ('queued','ok','warn','failed')),
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_sources_created ON doc_sources(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_sources_file_sha ON doc_sources(file_sha256)")

    # Invoice ingest records (photo + OCR)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS invoice_ingests (
        id INTEGER PRIMARY KEY,
        vendor_id INTEGER,
        vendor_guess_text TEXT,
        confidence REAL DEFAULT 0.0,
        telegram_chat_id INTEGER,
        telegram_user_id INTEGER,
        image_path TEXT,
        raw_ocr_text TEXT,
        status TEXT DEFAULT 'pending_vendor' CHECK(status IN ('pending_vendor','parsed','failed')),
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(vendor_id) REFERENCES vendors(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS invoice_line_items (
        id INTEGER PRIMARY KEY,
        invoice_ingest_id INTEGER NOT NULL,
        normalized_item_name TEXT,
        raw_line_text TEXT,
        quantity REAL,
        unit TEXT,
        canonical_value REAL,
        canonical_unit TEXT,
        display_original TEXT,
        display_pretty TEXT,
        unit_cost REAL,
        total_cost REAL,
        matched_inventory_item_id INTEGER,
        matched_vendor_item_id INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(invoice_ingest_id) REFERENCES invoice_ingests(id) ON DELETE CASCADE,
        FOREIGN KEY(matched_inventory_item_id) REFERENCES inventory_items(id),
        FOREIGN KEY(matched_vendor_item_id) REFERENCES vendor_items(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS vendor_item_affinity (
        id INTEGER PRIMARY KEY,
        normalized_item_name TEXT NOT NULL,
        vendor_id INTEGER NOT NULL,
        last_seen_at TEXT DEFAULT CURRENT_TIMESTAMP,
        purchase_count INTEGER DEFAULT 0,
        score REAL DEFAULT 0.0,
        UNIQUE(normalized_item_name, vendor_id),
        FOREIGN KEY(vendor_id) REFERENCES vendors(id) ON DELETE CASCADE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chat_vendor_context (
        id INTEGER PRIMARY KEY,
        telegram_chat_id INTEGER UNIQUE NOT NULL,
        last_vendor_id INTEGER,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(last_vendor_id) REFERENCES vendors(id)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS vendor_cutoff_reminders (
        id INTEGER PRIMARY KEY,
        vendor_id INTEGER NOT NULL,
        reminder_date TEXT NOT NULL,
        offset_minutes INTEGER NOT NULL,
        sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(vendor_id, reminder_date, offset_minutes),
        FOREIGN KEY(vendor_id) REFERENCES vendors(id) ON DELETE CASCADE
    );
    """)

    # Shopping list migrations for vendor-routed ordering.
    cur.execute("PRAGMA table_info(shopping_list)")
    shopping_cols = [row[1] for row in cur.fetchall()]
    if "vendor_id" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN vendor_id INTEGER REFERENCES vendors(id)")
    if "normalized_item_name" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN normalized_item_name TEXT")
    if "canonical_value" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN canonical_value REAL")
    if "canonical_unit" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN canonical_unit TEXT")
    if "display_original" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN display_original TEXT")
    if "display_pretty" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN display_pretty TEXT")
    if "status" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN status TEXT DEFAULT 'pending'")
    if "ordered_at" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN ordered_at TEXT")
    if "telegram_chat_id" not in shopping_cols:
        cur.execute("ALTER TABLE shopping_list ADD COLUMN telegram_chat_id INTEGER")

    # Invoice line-item canonical quantity migrations.
    cur.execute("PRAGMA table_info(invoice_line_items)")
    invoice_cols = [row[1] for row in cur.fetchall()]
    if invoice_cols and "canonical_value" not in invoice_cols:
        cur.execute("ALTER TABLE invoice_line_items ADD COLUMN canonical_value REAL")
    if invoice_cols and "canonical_unit" not in invoice_cols:
        cur.execute("ALTER TABLE invoice_line_items ADD COLUMN canonical_unit TEXT")
    if invoice_cols and "display_original" not in invoice_cols:
        cur.execute("ALTER TABLE invoice_line_items ADD COLUMN display_original TEXT")
    if invoice_cols and "display_pretty" not in invoice_cols:
        cur.execute("ALTER TABLE invoice_line_items ADD COLUMN display_pretty TEXT")

    # Prep-List migrations (station-first production fields).
    cur.execute("PRAGMA table_info(prep_list_items)")
    prep_cols = [row[1] for row in cur.fetchall()]
    if "station_id" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN station_id INTEGER REFERENCES stations(id)")
    if "target_quantity" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN target_quantity REAL")
    if "completed_quantity" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN completed_quantity REAL DEFAULT 0")
    if "display_unit" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN display_unit TEXT")
    if "assigned_staff_id" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN assigned_staff_id INTEGER REFERENCES staff(id)")
    if "last_update_at" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN last_update_at TEXT")
    if "last_update_by" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN last_update_by TEXT")
    if "hold_reason" not in prep_cols:
        cur.execute("ALTER TABLE prep_list_items ADD COLUMN hold_reason TEXT")
    cur.execute(
        """
        UPDATE prep_list_items
        SET target_quantity = COALESCE(target_quantity, need_quantity),
            completed_quantity = COALESCE(completed_quantity, 0),
            display_unit = COALESCE(display_unit, unit),
            last_update_at = COALESCE(last_update_at, created_at)
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prep_list_items_status_station ON prep_list_items(status, station_id)")

    # Staff chat-role mapping migration.
    cur.execute("PRAGMA table_info(staff)")
    staff_cols = [row[1] for row in cur.fetchall()]
    if "telegram_chat_id" not in staff_cols:
        cur.execute("ALTER TABLE staff ADD COLUMN telegram_chat_id INTEGER")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_staff_telegram_chat ON staff(telegram_chat_id)")

    # Ingest job table migrations.
    cur.execute("PRAGMA table_info(ingest_jobs)")
    ingest_cols = [row[1] for row in cur.fetchall()]
    if ingest_cols and "promoted_count" not in ingest_cols:
        cur.execute("ALTER TABLE ingest_jobs ADD COLUMN promoted_count INTEGER DEFAULT 0")
    if ingest_cols and "needs_review_count" not in ingest_cols:
        cur.execute("ALTER TABLE ingest_jobs ADD COLUMN needs_review_count INTEGER DEFAULT 0")
    if ingest_cols and "created_at" not in ingest_cols:
        cur.execute("ALTER TABLE ingest_jobs ADD COLUMN created_at TEXT")
    if ingest_cols:
        cur.execute(
            """
            UPDATE ingest_jobs
            SET created_at = COALESCE(created_at, started_at, updated_at, CURRENT_TIMESTAMP)
            WHERE created_at IS NULL
            """
        )

    # doc_sources migration: add updated_at and queued status support.
    cur.execute("PRAGMA table_info(doc_sources)")
    doc_source_cols = [row[1] for row in cur.fetchall()]
    if doc_source_cols and "updated_at" not in doc_source_cols:
        cur.execute("ALTER TABLE doc_sources ADD COLUMN updated_at TEXT")
    if doc_source_cols:
        cur.execute(
            """
            UPDATE doc_sources
            SET updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)
            WHERE updated_at IS NULL
            """
        )

    # SQLite does not support altering CHECK constraints; rebuild table if queued is unsupported.
    ds_sql_row = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='doc_sources'"
    ).fetchone()
    ds_sql = str(ds_sql_row[0] or "") if ds_sql_row else ""
    if ds_sql and "'queued'" not in ds_sql:
        cur.execute("ALTER TABLE doc_sources RENAME TO doc_sources_legacy")
        cur.execute("""
        CREATE TABLE doc_sources (
            id INTEGER PRIMARY KEY,
            ingest_id TEXT UNIQUE NOT NULL,
            filename TEXT NOT NULL,
            source_type TEXT DEFAULT 'unknown' CHECK(source_type IN ('restaurant_recipes','general_knowledge','general_knowledge_web','unknown')),
            restaurant_tag TEXT,
            file_sha256 TEXT,
            file_size INTEGER DEFAULT 0,
            extracted_text_chars INTEGER DEFAULT 0,
            chunk_count INTEGER DEFAULT 0,
            chunks_added INTEGER DEFAULT 0,
            status TEXT DEFAULT 'queued' CHECK(status IN ('queued','ok','warn','failed')),
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)
        cur.execute(
            """
            INSERT INTO doc_sources (
                id, ingest_id, filename, source_type, restaurant_tag, file_sha256, file_size,
                extracted_text_chars, chunk_count, chunks_added, status, created_at, updated_at
            )
            SELECT
                id, ingest_id, filename, source_type, restaurant_tag, file_sha256, file_size,
                extracted_text_chars, chunk_count, chunks_added,
                CASE
                    WHEN status IN ('ok','warn','failed') THEN status
                    ELSE 'queued'
                END,
                created_at,
                COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)
            FROM doc_sources_legacy
            """
        )
        cur.execute("DROP TABLE doc_sources_legacy")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_sources_created ON doc_sources(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_sources_file_sha ON doc_sources(file_sha256)")

    # Ensure autonomy_status row exists after upgrades.
    cur.execute("INSERT OR IGNORE INTO autonomy_status (id, is_running) VALUES (1, 0)")

    # Canonicalize receiving logs:
    # move legacy dashboard table receiving_logs -> receiving_log and then drop the duplicate table.
    legacy_receiving_exists = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='receiving_logs' LIMIT 1"
    ).fetchone()
    if legacy_receiving_exists:
        legacy_rows = cur.execute(
            "SELECT date, supplier, invoice_number, total_amount, has_issue, notes, created_at FROM receiving_logs"
        ).fetchall()
        for row in legacy_rows:
            supplier = (row["supplier"] or "").strip() or "Legacy Receiving Entry"
            issue_text = "Issue flagged." if row["has_issue"] else ""
            note_text = " ".join(
                part for part in [(row["notes"] or "").strip(), issue_text] if part
            ).strip() or None
            cur.execute(
                """
                INSERT INTO receiving_log (
                    vendor_id, invoice_number, item_name, quantity_received, unit, unit_cost, total_cost,
                    temperature_check, quality_ok, notes, received_by, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """,
                (
                    None,
                    row["invoice_number"],
                    supplier,
                    None,
                    None,
                    None,
                    row["total_amount"],
                    None,
                    0 if row["has_issue"] else 1,
                    note_text,
                    "legacy_migration",
                    row["created_at"],
                ),
            )
        cur.execute("DROP TABLE IF EXISTS receiving_logs")

    con.commit()
    con.close()

def get_or_create_user(telegram_user_id: int, display_name: str) -> None:
    con = _conn()
    con.execute(
        "INSERT OR IGNORE INTO users (telegram_user_id, display_name) VALUES (?, ?)",
        (telegram_user_id, display_name),
    )
    con.commit()
    con.close()

def get_or_create_active_session(chat_id: int, user_id: int) -> int:
    con = _conn()
    cur = con.cursor()

    cur.execute(
        "SELECT id FROM sessions WHERE telegram_chat_id=? AND telegram_user_id=? AND is_active=1 ORDER BY id DESC LIMIT 1",
        (chat_id, user_id),
    )
    row = cur.fetchone()
    if row:
        con.close()
        return int(row[0])

    cur.execute(
        "INSERT INTO sessions (telegram_chat_id, telegram_user_id, title, is_active) VALUES (?, ?, ?, 1)",
        (chat_id, user_id, "default"),
    )
    con.commit()
    session_id = int(cur.lastrowid)
    con.close()
    return session_id

def add_message(session_id: int, role: str, content: str) -> None:
    con = _conn()
    con.execute(
        "INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)",
        (session_id, role, content),
    )
    con.commit()
    con.close()

def get_recent_messages(session_id: int, limit: int = 16) -> List[Tuple[str, str]]:
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT role, content FROM messages WHERE session_id=? ORDER BY id DESC LIMIT ?",
        (session_id, limit),
    )
    rows = cur.fetchall()
    con.close()
    rows.reverse()
    return [(r[0], r[1]) for r in rows]
