use crate::archival::utils::{get_first_id_to_start_notifier_from, is_row_exists};
use log::{debug, info};
use sqlx::{Error, PgPool};

/// Keeps the current cursor of the `internet_archive_urls` table and notify the archiver's listener
/// when new urls are available to process
pub struct Notifier {
    /// Notify from this row in the table `internet_archive_urls`
    start_notifier_from: Option<i32>,

    /// Database pool
    pool: PgPool,
}

impl Notifier {
    pub async fn new(pool: PgPool) -> Notifier {
        let start_notifier_from = get_first_id_to_start_notifier_from(pool.clone())
            .await
            .inspect(|id| info!("[NOTIFIER] Starting from row id: {id}"));

        Notifier {
            start_notifier_from,
            pool,
        }
    }

    /// Send a notification to the archiver's listener with the current row using `pg_notify`.
    pub async fn notify(&mut self) -> Result<(), Error> {
        match self.start_notifier_from {
            Some(current_id) => {
                let pool = self.pool.clone();

                // Send a notification using a postgreSQL function
                sqlx::query("SELECT external_url_archiver.notify_archive_urls($1)")
                    .bind(current_id)
                    .execute(&pool)
                    .await?;

                info!("[NOTIFIER] Adding internet_archive_urls id {current_id} to the archive_urls channel");

                //Case: If the notifier reached the end of the row, and couldn't find any unarchived row in Internet Archives URL table, we will not increment the self.start_notifier_from count
                if is_row_exists(&pool, current_id).await {
                    //TODO: BUG! Having a field serial does NOT garanty that id + 1 exists, and is the last row of the table!
                    // (Ex: Delete and upsert disrupting the flow).
                    // This is saved by `get_first_id_to_start_notifier_from()` but still bad!
                    self.start_notifier_from = Some(current_id + 1);
                }
                Ok(())
            }
            None => {
                // We called `notify` without having a current row as our cursor
                // So we go fetch the next unarchived row (if it exists)
                debug!("[NOTIFIER] Tried to notify, but no row is set to archive. Searching for a new row");
                self.start_notifier_from =
                    get_first_id_to_start_notifier_from(self.pool.clone()).await;
                Ok(())
            }
        }
    }

    /// Return true if there's a row to notify, and thus need to call [`notify`]
    pub async fn should_notify(&mut self) -> bool {
        match self.start_notifier_from {
            Some(id) => is_row_exists(&self.pool, id).await,
            None => true,
        }
    }

    //TODO: Silence clippy warning when removing the underscore. Require: Moving integration tests to 
    pub fn _get_notifier_index(&self) -> i32 {
        self.start_notifier_from.unwrap() //TODO: BAD! It's only for tests, but may be used in actual code!
    }
}
