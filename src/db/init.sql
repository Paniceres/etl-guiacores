-- Conectar a la base de datos
\c guiacores;

-- Tabla de fuentes de datos
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,
    source_name VARCHAR(255) NOT NULL,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Tabla de datos crudos
CREATE TABLE raw_leads (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(50),
    source_name VARCHAR(255),
    raw_data JSONB,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    etl_status VARCHAR(20) DEFAULT 'pending',
    etl_notes TEXT
);

-- Tabla de negocios procesados
CREATE TABLE businesses (
    id SERIAL PRIMARY KEY,
    id_negocio VARCHAR(50) UNIQUE,
    nombre VARCHAR(255),
    direccion TEXT,
    telefonos TEXT,
    whatsapp VARCHAR(50),
    sitio_web VARCHAR(255),
    email VARCHAR(255),
    facebook VARCHAR(255),
    instagram VARCHAR(255),
    horarios TEXT,
    rubros TEXT,
    descripcion TEXT,
    servicios TEXT,
    latitud VARCHAR(50),
    longitud VARCHAR(50),
    url VARCHAR(255),
    fecha_extraccion TIMESTAMPTZ,
    fecha_actualizacion TIMESTAMPTZ DEFAULT NOW(),
    id_data_source INTEGER REFERENCES data_sources(source_id)
);

-- Tabla de logs de ETL
CREATE TABLE etl_runs (
    run_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR(20),
    raw_records_processed INTEGER,
    businesses_created INTEGER,
    duplicates_found INTEGER,
    validation_errors INTEGER,
    data_source_ids_included INTEGER[],
    error_message TEXT,
    duration_seconds INTEGER
);

-- Índices
CREATE INDEX idx_raw_leads_source_type ON raw_leads(source_type);
CREATE INDEX idx_raw_leads_etl_status ON raw_leads(etl_status);
CREATE INDEX idx_businesses_id_negocio ON businesses(id_negocio);
CREATE INDEX idx_businesses_nombre ON businesses(nombre);
CREATE INDEX idx_businesses_rubros ON businesses(rubros);
CREATE INDEX idx_etl_runs_timestamp ON etl_runs(run_timestamp);

-- Insertar fuente de datos inicial para GuiaCores
INSERT INTO data_sources (source_type, source_name, notes)
VALUES ('web_scraping', 'GuiaCores', 'Datos extraídos de GuiaCores.com.ar'); 