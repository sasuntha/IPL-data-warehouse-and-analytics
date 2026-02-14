SET search_path TO ipl_analytics;
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    season VARCHAR(10) NOT NULL,
    day_of_week VARCHAR(10),
    day_name VARCHAR(15),
    week_of_year SMALLINT,
    month_name VARCHAR(15),
    quarter SMALLINT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    CONSTRAINT chk_day CHECK (
        day BETWEEN 1 AND 31
    ),
    CONSTRAINT chk_month CHECK (
        month BETWEEN 1 AND 12
    ),
    CONSTRAINT chk_quarter CHECK (
        quarter BETWEEN 1 AND 4
    ),
    CONSTRAINT chk_week CHECK (
        week_of_year BETWEEN 1 AND 53
    )
);
CREATE INDEX idx_date_season ON dim_date(season);
CREATE INDEX idx_date_year ON dim_date(year);
CREATE INDEX idx_date_full ON dim_date(full_date);
COMMENT ON TABLE dim_date IS 'Date dimension with calendar attributes';
CREATE TABLE dim_player (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(100) NOT NULL UNIQUE,
    player_role VARCHAR(30),
    nationality VARCHAR(50) DEFAULT 'India',
    batting_style VARCHAR(30),
    bowling_style VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    debut_year SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_player_name ON dim_player(player_name);
CREATE INDEX idx_player_role ON dim_player(player_role);
CREATE INDEX idx_player_active ON dim_player(is_active);
COMMENT ON TABLE dim_player IS 'Player dimension with career details';
CREATE TABLE dim_team (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(100) NOT NULL UNIQUE,
    team_short_name VARCHAR(10),
    home_city VARCHAR(50),
    team_color VARCHAR(30),
    franchise_owner VARCHAR(100),
    established_year SMALLINT,
    is_active BOOLEAN DEFAULT TRUE,
    championships_won SMALLINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_team_name ON dim_team(team_name);
CREATE INDEX idx_team_city ON dim_team(home_city);
CREATE INDEX idx_team_active ON dim_team(is_active);
COMMENT ON TABLE dim_team IS 'Team dimension with franchise details';
CREATE TABLE dim_venue (
    venue_id SERIAL PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50) DEFAULT 'India',
    capacity INTEGER,
    established_year SMALLINT,
    pitch_type VARCHAR(30),
    typical_score SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_venue UNIQUE (venue_name, city)
);
CREATE INDEX idx_venue_city ON dim_venue(city);
CREATE INDEX idx_venue_country ON dim_venue(country);
CREATE INDEX idx_venue_name ON dim_venue(venue_name);
COMMENT ON TABLE dim_venue IS 'Venue dimension with stadium details';
CREATE TABLE dim_umpire (
    umpire_id SERIAL PRIMARY KEY,
    umpire_name VARCHAR(100) NOT NULL UNIQUE,
    nationality VARCHAR(50),
    experience_years SMALLINT,
    is_elite_panel BOOLEAN DEFAULT FALSE,
    total_matches INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_umpire_name ON dim_umpire(umpire_name);
CREATE INDEX idx_umpire_panel ON dim_umpire(is_elite_panel);
COMMENT ON TABLE dim_umpire IS 'Umpire dimension';
CREATE TABLE dim_match (
    match_id INTEGER PRIMARY KEY,
    match_type VARCHAR(20),
    balls_per_over SMALLINT DEFAULT 6,
    gender VARCHAR(10),
    team_type VARCHAR(20),
    match_number SMALLINT,
    event_stage VARCHAR(30),
    CONSTRAINT chk_gender CHECK (gender IN ('Male', 'Female', 'Mixed')),
    CONSTRAINT chk_balls_per_over CHECK (balls_per_over IN (6, 8))
);
CREATE INDEX idx_match_type ON dim_match(match_type);
CREATE INDEX idx_match_gender ON dim_match(gender);
CREATE INDEX idx_match_stage ON dim_match(event_stage);
COMMENT ON TABLE dim_match IS 'Match metadata dimension';
CREATE TABLE dim_event (
    event_id SERIAL PRIMARY KEY,
    event_name VARCHAR(100) NOT NULL,
    event_year SMALLINT NOT NULL,
    event_type VARCHAR(30),
    total_matches SMALLINT,
    start_date DATE,
    end_date DATE,
    CONSTRAINT uk_event UNIQUE (event_name, event_year)
);
CREATE INDEX idx_event_year ON dim_event(event_year);
CREATE INDEX idx_event_name ON dim_event(event_name);
COMMENT ON TABLE dim_event IS 'Tournament/Event dimension';