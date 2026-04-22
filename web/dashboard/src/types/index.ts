export interface TaskConfig {
  table_name: string;
  sql: string;
  source_db: string;
  target_db: string;
  ignore: boolean;
  mode: string;
  batch_size: number;
  max_retries: number;
  validate: string;
  validate_sample_size: string;
  merge_keys: string[];
  resume_key: string;
  resume_from: string;
  state_file: string;
  allow_same_table: boolean;
  skip_create_table: boolean;
  schema_evolution: boolean;
  dlq_path: string;
  dlq_format: string;
  indexes: IndexConfig[];
  masking: MaskingConfig[];
  pre_sql: string[];
  post_sql: string[];
  depends_on: string[];
}

export interface IndexConfig {
  name: string;
  columns: string[];
}

export interface MaskingConfig {
  column: string;
  rule: string;
}

export interface TaskResponse extends TaskConfig {
  status: string;
  processed: number;
  percentage: number;
  duration_ms: number;
}

export interface DatabaseConfig {
  name: string;
  type: string;
  host: string;
  port: string;
  service: string;
  database: string;
  user: string;
  password: string;
  path: string;
  ssl_mode: string;
  pool_max_open: number;
  pool_max_idle: number;
}

export interface MigrationRecord {
  id: string;
  config_hash: string;
  started_at: string;
  finished_at: string | null;
  task_name: string;
  source_db: string;
  target_db: string;
  mode: string;
  rows_processed: number;
  rows_failed: number;
  validation_result: string;
  error_message: string;
  version: string;
}

export interface TaskProgressData {
  task: string;
  source_db: string;
  target_db: string;
  estimated_rows: number;
  processed: number;
  percentage: number;
  duration_ms: number;
  error: string;
}

export interface CheckResult {
  name: string;
  status: number;
  message: string;
}

export interface TableSchema {
  columns: ColumnMetadata[];
  primary_key: string[];
  indexes: IndexInfo[];
}

export interface ColumnMetadata {
  name: string;
  transform: string;
  database_type: string;
  length: number;
  precision: number;
  scale: number;
  length_valid: boolean;
  precision_scale_valid: boolean;
  nullable: boolean;
  nullable_valid: boolean;
  go_type: string;
}

export interface IndexInfo {
  name: string;
  columns: string[];
  unique: boolean;
}

export interface DaemonStatus {
  running: boolean;
  last_error?: string;
}
