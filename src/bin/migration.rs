use app_lib::config;

use diesel::{pg, Connection};

use diesel_migrations::{FileBasedMigrations, MigrationHarness};
use std::{convert::TryInto, env};

enum Action {
    Up,
    Down,
}

#[derive(Debug)]
struct Error(String);

impl TryInto<Action> for String {
    type Error = Error;

    fn try_into(self) -> Result<Action, Self::Error> {
        match &self[..] {
            "up" => Ok(Action::Up),
            "down" => Ok(Action::Down),
            _ => Err(Error("cannot parse command line arg".into())),
        }
    }
}

fn main() {
    let action: Action = env::args().nth(1).unwrap().try_into().unwrap();

    let config = config::load_migration_config().unwrap();

    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.postgres.user,
        config.postgres.password,
        config.postgres.host,
        config.postgres.port,
        config.postgres.database
    );

    let mut conn = pg::PgConnection::establish(&db_url).unwrap();

    let migrations = FileBasedMigrations::find_migrations_directory().unwrap();

    match action {
        Action::Up => {
            conn.run_pending_migrations(migrations).unwrap();
        }
        Action::Down => {
            conn.revert_last_migration(migrations).unwrap();
        }
    };
}
