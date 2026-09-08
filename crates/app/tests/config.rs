use std::path::PathBuf;

use autosre_app::config::{Config, ConfigError, Env};
use tempfile::TempDir;

/// Файл настроек во временном каталоге, живущий столько же, сколько каталог.
struct Settings {
    path: PathBuf,
    _directory: TempDir,
}

impl Settings {
    fn of(body: &str) -> Self {
        let directory = TempDir::new().expect("временный каталог не создан");
        let path = directory.path().join("autosre.toml");
        std::fs::write(&path, body).expect("файл настроек не записан");
        Self {
            path,
            _directory: directory,
        }
    }

    fn read(&self, env: &dyn Env) -> Result<Config, ConfigError> {
        Config::read(&self.path, env)
    }
}

/// Окружение, в котором есть ровно перечисленные ключи.
struct Keys(Vec<(&'static str, &'static str)>);

impl Env for Keys {
    fn var(&self, key: &str) -> Option<String> {
        self.0
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
    }
}

fn full() -> Keys {
    Keys(vec![
        ("AUTOSRE_MODEL_KEY", "sk-Q7f3-ephemeral"),
        ("AUTOSRE_SESSION_KEY", "9f3a1c7e0b"),
    ])
}

/// Наименьший осмысленный файл: без него агенту нечего делать.
const ENOUGH: &str = r#"
[logs]
url = "http://192.0.2.12:9428"

[metrics]
url = "http://192.0.2.11:8428"

[model]
url = "http://192.0.2.10:4000"
name = "gemma-4-12B-it-qat-q4_0-gguf"

[[horizon]]
name = "15m"
width = "15m"
period = "1m"
"#;

/// Файл секретов рядом с файлом настроек.
fn envfile(settings: &Settings, body: &str) -> PathBuf {
    let path = settings.path.with_file_name("autosre.env");
    std::fs::write(&path, body).expect("файл секретов не записан");
    path
}

#[test]
fn takes_a_missing_secret_from_the_env_file_beside_the_settings() {
    let settings = Settings::of(ENOUGH);
    envfile(
        &settings,
        "AUTOSRE_MODEL_KEY=sk-from-file\nAUTOSRE_SESSION_KEY=s3ss\n",
    );
    let none = Keys(vec![]);
    let env = autosre_app::config::Envfile::beside(&none, &settings.path);
    assert_eq!(settings.read(&env).unwrap().secrets.model, "sk-from-file");
}

#[test]
fn prefers_the_process_environment_to_the_file() {
    let settings = Settings::of(ENOUGH);
    envfile(
        &settings,
        "AUTOSRE_MODEL_KEY=sk-from-file\nAUTOSRE_SESSION_KEY=s3ss\n",
    );
    let keys = full();
    let env = autosre_app::config::Envfile::beside(&keys, &settings.path);
    assert_eq!(
        settings.read(&env).unwrap().secrets.model,
        "sk-Q7f3-ephemeral"
    );
}

#[test]
fn reads_the_env_file_past_comments_and_quotes() {
    let settings = Settings::of(ENOUGH);
    envfile(
        &settings,
        "# секреты\nAUTOSRE_MODEL_KEY=\"sk-quoted\"\n\nAUTOSRE_SESSION_KEY = 's3ss'\n",
    );
    let none = Keys(vec![]);
    let env = autosre_app::config::Envfile::beside(&none, &settings.path);
    let secrets = settings.read(&env).unwrap().secrets;
    assert_eq!(
        (secrets.model.as_str(), secrets.session.as_str()),
        ("sk-quoted", "s3ss")
    );
}

#[test]
fn treats_an_empty_value_in_the_file_as_missing() {
    let settings = Settings::of(ENOUGH);
    envfile(&settings, "AUTOSRE_MODEL_KEY=\nAUTOSRE_SESSION_KEY=s3ss\n");
    let none = Keys(vec![]);
    let env = autosre_app::config::Envfile::beside(&none, &settings.path);
    assert!(matches!(
        settings.read(&env).unwrap_err(),
        ConfigError::Missing("AUTOSRE_MODEL_KEY")
    ));
}

#[test]
fn lives_without_the_env_file() {
    let settings = Settings::of(ENOUGH);
    let keys = full();
    let env = autosre_app::config::Envfile::beside(&keys, &settings.path);
    assert!(settings.read(&env).is_ok());
}

#[test]
fn reads_the_bind_address() {
    let settings = Settings::of(&format!("bind = \"127.0.0.1:8791\"\n{ENOUGH}"));
    assert_eq!(
        settings.read(&full()).unwrap().file.bind.to_string(),
        "127.0.0.1:8791"
    );
}

#[test]
fn falls_back_to_the_usual_port() {
    let settings = Settings::of(ENOUGH);
    assert_eq!(settings.read(&full()).unwrap().file.bind.port(), 8096);
}

#[test]
fn keeps_the_own_streams_of_the_agent() {
    let settings = Settings::of(
        r#"
[logs]
url = "http://192.0.2.12:9428"
self_streams = ['{container="autosre"}', '{container="litellm"}']

[metrics]
url = "http://192.0.2.11:8428"

[model]
url = "http://192.0.2.10:4000"
name = "gemma"

[[horizon]]
name = "15m"
width = "15m"
period = "1m"
"#,
    );
    assert_eq!(
        settings.read(&full()).unwrap().file.logs.self_streams.len(),
        2
    );
}

#[test]
fn refuses_to_start_without_the_model_key() {
    let settings = Settings::of(ENOUGH);
    let keys = Keys(vec![("AUTOSRE_SESSION_KEY", "9f3a1c7e0b")]);
    assert!(matches!(
        settings.read(&keys).unwrap_err(),
        ConfigError::Missing("AUTOSRE_MODEL_KEY")
    ));
}

#[test]
fn refuses_to_start_without_the_session_key() {
    let settings = Settings::of(ENOUGH);
    let keys = Keys(vec![("AUTOSRE_MODEL_KEY", "sk-Q7f3-ephemeral")]);
    assert!(matches!(
        settings.read(&keys).unwrap_err(),
        ConfigError::Missing("AUTOSRE_SESSION_KEY")
    ));
}

#[test]
fn treats_a_blank_key_as_absent() {
    let settings = Settings::of(ENOUGH);
    let keys = Keys(vec![
        ("AUTOSRE_MODEL_KEY", "   "),
        ("AUTOSRE_SESSION_KEY", "9f3a1c7e0b"),
    ]);
    assert!(matches!(
        settings.read(&keys).unwrap_err(),
        ConfigError::Missing("AUTOSRE_MODEL_KEY")
    ));
}

#[test]
fn names_the_unknown_setting() {
    let settings = Settings::of(&format!("telepathy = true\n{ENOUGH}"));
    assert_eq!(settings.read(&full()).unwrap().unknown, vec!["telepathy"]);
}

#[test]
fn names_the_unknown_setting_of_a_section() {
    let settings = Settings::of(&format!(
        "{ENOUGH}\n[digging]\nparallel = 2\nspeed = \"much\"\n"
    ));
    assert_eq!(
        settings.read(&full()).unwrap().unknown,
        vec!["digging.speed"]
    );
}

#[test]
fn names_the_gone_queue_section() {
    let settings = Settings::of(&format!("{ENOUGH}\n[queue]\nparallel = 2\n"));
    assert_eq!(settings.read(&full()).unwrap().unknown, vec!["queue"]);
}

#[test]
fn keeps_quiet_when_everything_is_known() {
    let settings = Settings::of(ENOUGH);
    assert!(settings.read(&full()).unwrap().unknown.is_empty());
}

#[test]
fn refuses_a_config_without_horizons() {
    let settings = Settings::of(
        r#"
[logs]
url = "http://192.0.2.12:9428"

[metrics]
url = "http://192.0.2.11:8428"

[model]
url = "http://192.0.2.10:4000"
name = "gemma"
"#,
    );
    assert!(matches!(
        settings.read(&full()).unwrap_err(),
        ConfigError::Invalid(_)
    ));
}

#[test]
fn refuses_a_period_wider_than_the_window() {
    let settings = Settings::of(&ENOUGH.replace("period = \"1m\"", "period = \"30m\""));
    assert!(matches!(
        settings.read(&full()).unwrap_err(),
        ConfigError::Invalid(_)
    ));
}

#[test]
fn refuses_a_queue_without_slots() {
    let settings = Settings::of(&format!("{ENOUGH}\n[digging]\nparallel = 0\n"));
    assert!(matches!(
        settings.read(&full()).unwrap_err(),
        ConfigError::Invalid(_)
    ));
}

#[test]
fn refuses_an_unreadable_duration() {
    let settings = Settings::of(&ENOUGH.replace("width = \"15m\"", "width = \"скоро\""));
    assert!(matches!(
        settings.read(&full()).unwrap_err(),
        ConfigError::Parse(_)
    ));
}

#[test]
fn counts_only_the_enabled_horizons() {
    let settings = Settings::of(&format!(
        "{ENOUGH}\n[[horizon]]\nname = \"5m\"\nwidth = \"5m\"\nperiod = \"1m\"\nenabled = false\n"
    ));
    assert_eq!(settings.read(&full()).unwrap().file.enabled().len(), 1);
}

#[test]
fn complains_about_a_missing_file() {
    let directory = TempDir::new().expect("временный каталог не создан");
    assert!(matches!(
        Config::read(&directory.path().join("нет.toml"), &full()).unwrap_err(),
        ConfigError::Read { .. }
    ));
}

#[test]
fn reads_the_shipped_example() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/autosre.toml");
    assert!(Config::read(&path, &full()).unwrap().unknown.is_empty());
}

#[test]
fn digs_deep_enough_for_the_shortest_horizon() {
    let settings = Settings::of(&format!(
        "{ENOUGH}history = 24\n\n[collector]\nbackfill = \"2h\"\n"
    ));
    let config = settings.read(&full()).unwrap();
    assert_eq!(config.file.depth().as_secs() / 60, 375);
}

#[test]
fn keeps_a_backfill_deeper_than_the_baseline() {
    let settings = Settings::of(&format!(
        "{ENOUGH}history = 24\n\n[collector]\nbackfill = \"24h\"\n"
    ));
    let config = settings.read(&full()).unwrap();
    assert_eq!(config.file.depth().as_secs() / 3600, 24);
}
