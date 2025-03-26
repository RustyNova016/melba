use core::cell::LazyCell;
use core::num::NonZeroU32;

use governor::clock::QuantaClock;
use governor::clock::QuantaInstant;
use governor::middleware::NoOpMiddleware;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use governor::Quota;
use governor::RateLimiter;

use crate::archival::archival_response::ArchivalErrorResponse;
use crate::archival::archival_response::ArchivalResponse;
use crate::archival::client::REQWEST_CLIENT;
use crate::archival::error::ArchivalError;
use crate::configuration::SETTINGS;

pub const IA_SAVE_RATELIMIT: LazyCell<
    RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>,
> = LazyCell::new(|| {
    RateLimiter::direct(
        Quota::per_minute(NonZeroU32::new(SETTINGS.wayback_machine_api.save_rate_limit).unwrap())
            .allow_burst(NonZeroU32::new(1).unwrap()),
    )
});

///Handles the network request to archive the URL
pub async fn archive_url_in_ia(url: &str) -> Result<ArchivalResponse, ArchivalError> {
    IA_SAVE_RATELIMIT.until_ready().await;
    let response = REQWEST_CLIENT
        .post(&SETTINGS.wayback_machine_api.save_endpoint_url)
        .body(format!("url={}", url))
        .send()
        .await?;

    let res = response.text().await?;

    // IA might return either a Ok response, or a JSON response, and occasionally a html response.
    // So we sort those in their proper types
    if let Ok(val) = serde_json::from_str(&res) {
        Ok(val)
    } else if let Ok(err) = serde_json::from_str::<ArchivalErrorResponse>(&res) {
        Err(ArchivalError::WaybackMachineErr(err))
    } else {
        Err(ArchivalError::WaybackMachineErrStr(res))
    }
}
