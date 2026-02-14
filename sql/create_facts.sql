SET search_path TO ipl_analytics;
CREATE TABLE fact_ball_delivery (
    delivery_id BIGSERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES dim_match(match_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    -- Player Foreign Keys
    batter_id INTEGER NOT NULL REFERENCES dim_player(player_id),
    bowler_id INTEGER NOT NULL REFERENCES dim_player(player_id),
    non_striker_id INTEGER REFERENCES dim_player(player_id),
    -- Team Foreign Keys
    batting_team_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    bowling_team_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    -- Other Foreign Keys
    venue_id INTEGER NOT NULL REFERENCES dim_venue(venue_id),
    umpire_id INTEGER REFERENCES dim_umpire(umpire_id),
    -- Degenerate Dimensions (Context)
    innings SMALLINT NOT NULL,
    over_number SMALLINT NOT NULL,
    ball_number SMALLINT NOT NULL,
    ball_sequence INTEGER NOT NULL,
    bat_position SMALLINT,
    non_striker_position SMALLINT,
    -- Match Phase Classification
    match_phase VARCHAR(20),
    -- Additive Measures
    runs_scored SMALLINT DEFAULT 0,
    runs_extras SMALLINT DEFAULT 0,
    runs_total SMALLINT DEFAULT 0,
    runs_bowler SMALLINT DEFAULT 0,
    balls_faced SMALLINT DEFAULT 1,
    -- Semi-Additive Measures (Point-in-time)
    runs_target SMALLINT,
    runs_required INTEGER,
    balls_remaining INTEGER,
    team_runs SMALLINT,
    team_balls SMALLINT,
    team_wickets SMALLINT,
    batter_runs SMALLINT,
    batter_balls SMALLINT,
    bowler_wickets SMALLINT,
    -- Calculated Metrics
    current_run_rate DECIMAL(5, 2),
    required_run_rate DECIMAL(5, 2),
    pressure_index DECIMAL(6, 2),
    -- Flags (Non-Additive)
    is_valid_ball BOOLEAN DEFAULT TRUE,
    is_wicket BOOLEAN DEFAULT FALSE,
    is_boundary BOOLEAN DEFAULT FALSE,
    is_six BOOLEAN DEFAULT FALSE,
    is_four BOOLEAN DEFAULT FALSE,
    is_dot_ball BOOLEAN DEFAULT FALSE,
    is_new_batter BOOLEAN DEFAULT FALSE,
    is_striker_out BOOLEAN DEFAULT FALSE,
    -- Attributes
    extra_type VARCHAR(20),
    wicket_kind VARCHAR(30),
    player_out_id INTEGER REFERENCES dim_player(player_id),
    fielders VARCHAR(200),
    batting_partners VARCHAR(100),
    next_batter_id INTEGER REFERENCES dim_player(player_id),
    -- Constraints
    CONSTRAINT chk_innings CHECK (innings IN (1, 2, 3, 4)),
    CONSTRAINT chk_runs_scored CHECK (
        runs_scored BETWEEN 0 AND 7
    ),
    CONSTRAINT chk_over CHECK (
        over_number BETWEEN 0 AND 50
    ),
    CONSTRAINT chk_ball CHECK (
        ball_number BETWEEN 0 AND 10
    ),
    CONSTRAINT chk_phase CHECK (match_phase IN ('Powerplay', 'Middle', 'Death')),
    CONSTRAINT uk_delivery UNIQUE (match_id, innings, ball_sequence)
);
-- Partitioning by date_id for performance
CREATE INDEX idx_ball_match ON fact_ball_delivery(match_id);
CREATE INDEX idx_ball_date ON fact_ball_delivery(date_id);
CREATE INDEX idx_ball_batter ON fact_ball_delivery(batter_id);
CREATE INDEX idx_ball_bowler ON fact_ball_delivery(bowler_id);
CREATE INDEX idx_ball_teams ON fact_ball_delivery(batting_team_id, bowling_team_id);
CREATE INDEX idx_ball_venue ON fact_ball_delivery(venue_id);
CREATE INDEX idx_ball_phase ON fact_ball_delivery(match_phase);
CREATE INDEX idx_ball_wicket ON fact_ball_delivery(is_wicket)
WHERE is_wicket = TRUE;
CREATE INDEX idx_ball_boundary ON fact_ball_delivery(is_boundary)
WHERE is_boundary = TRUE;
-- Composite indexes for common queries
CREATE INDEX idx_ball_batter_stats ON fact_ball_delivery(batter_id, runs_scored, is_wicket, is_boundary);
CREATE INDEX idx_ball_bowler_stats ON fact_ball_delivery(bowler_id, runs_bowler, is_wicket, is_valid_ball);
CREATE INDEX idx_ball_match_innings ON fact_ball_delivery(match_id, innings, ball_sequence);
COMMENT ON TABLE fact_ball_delivery IS 'Granular fact table - one row per ball delivered';
CREATE TABLE fact_innings_summary (
    innings_id BIGSERIAL PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES dim_match(match_id),
    innings_number SMALLINT NOT NULL,
    -- Foreign Keys
    batting_team_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    bowling_team_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    venue_id INTEGER NOT NULL REFERENCES dim_venue(venue_id),
    -- Overall Metrics
    total_runs INTEGER NOT NULL,
    total_wickets SMALLINT NOT NULL,
    total_overs DECIMAL(4, 1) NOT NULL,
    total_balls INTEGER NOT NULL,
    run_rate DECIMAL(5, 2),
    -- Phase-wise Breakdown
    powerplay_runs SMALLINT,
    powerplay_wickets SMALLINT,
    powerplay_overs DECIMAL(3, 1),
    powerplay_run_rate DECIMAL(5, 2),
    middle_overs_runs SMALLINT,
    middle_overs_wickets SMALLINT,
    middle_overs_overs DECIMAL(4, 1),
    middle_overs_run_rate DECIMAL(5, 2),
    death_overs_runs SMALLINT,
    death_overs_wickets SMALLINT,
    death_overs_overs DECIMAL(3, 1),
    death_overs_run_rate DECIMAL(5, 2),
    -- Strategic Metrics
    dot_ball_count INTEGER,
    dot_ball_percentage DECIMAL(5, 2),
    boundary_count INTEGER,
    boundary_percentage DECIMAL(5, 2),
    fours INTEGER,
    sixes INTEGER,
    extras INTEGER,
    extras_percentage DECIMAL(5, 2),
    -- Partnership Metrics
    highest_partnership INTEGER,
    avg_partnership DECIMAL(6, 2),
    total_partnerships SMALLINT,
    -- Top Performers
    top_scorer_id INTEGER REFERENCES dim_player(player_id),
    top_scorer_runs SMALLINT,
    best_bowler_id INTEGER REFERENCES dim_player(player_id),
    best_bowler_wickets SMALLINT,
    CONSTRAINT uk_innings UNIQUE (match_id, innings_number)
);
CREATE INDEX idx_innings_match ON fact_innings_summary(match_id);
CREATE INDEX idx_innings_team ON fact_innings_summary(batting_team_id, bowling_team_id);
CREATE INDEX idx_innings_date ON fact_innings_summary(date_id);
CREATE INDEX idx_innings_venue ON fact_innings_summary(venue_id);
COMMENT ON TABLE fact_innings_summary IS 'Innings-level aggregated fact table';
CREATE TABLE fact_match_summary (
    match_id INTEGER PRIMARY KEY REFERENCES dim_match(match_id),
    -- Foreign Keys
    date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
    event_id INTEGER REFERENCES dim_event(event_id),
    venue_id INTEGER NOT NULL REFERENCES dim_venue(venue_id),
    team1_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    team2_id INTEGER NOT NULL REFERENCES dim_team(team_id),
    toss_winner_id INTEGER REFERENCES dim_team(team_id),
    match_winner_id INTEGER REFERENCES dim_team(team_id),
    player_of_match_id INTEGER REFERENCES dim_player(player_id),
    superover_winner_id INTEGER REFERENCES dim_team(team_id),
    -- Degenerate Dimensions
    match_number SMALLINT,
    event_match_number SMALLINT,
    stage VARCHAR(30),
    -- Match Outcome
    toss_decision VARCHAR(10),
    win_outcome VARCHAR(100),
    win_margin INTEGER,
    win_type VARCHAR(20),
    result_type VARCHAR(30),
    method VARCHAR(30),
    -- Team Totals
    team1_score INTEGER,
    team1_wickets SMALLINT,
    team1_overs DECIMAL(4, 1),
    team2_score INTEGER,
    team2_wickets SMALLINT,
    team2_overs DECIMAL(4, 1),
    -- Match Characteristics
    total_runs INTEGER,
    total_wickets SMALLINT,
    total_boundaries INTEGER,
    total_sixes INTEGER,
    is_high_scoring BOOLEAN,
    is_low_scoring BOOLEAN,
    is_close_match BOOLEAN,
    is_super_over BOOLEAN DEFAULT FALSE,
    -- Custom Metrics
    match_competitiveness_score DECIMAL(6, 2),
    batting_first_advantage DECIMAL(6, 2),
    CONSTRAINT chk_toss_decision CHECK (toss_decision IN ('bat', 'field')),
    CONSTRAINT chk_win_type CHECK (
        win_type IN ('runs', 'wickets', 'super_over', 'tie')
    )
);
CREATE INDEX idx_match_summary_date ON fact_match_summary(date_id);
CREATE INDEX idx_match_summary_event ON fact_match_summary(event_id);
CREATE INDEX idx_match_summary_venue ON fact_match_summary(venue_id);
CREATE INDEX idx_match_summary_teams ON fact_match_summary(team1_id, team2_id);
CREATE INDEX idx_match_summary_winner ON fact_match_summary(match_winner_id);
CREATE INDEX idx_match_summary_stage ON fact_match_summary(stage);
COMMENT ON TABLE fact_match_summary IS 'Match-level aggregated fact table';