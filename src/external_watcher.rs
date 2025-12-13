use anyhow::{anyhow, Result};
use std::{time::Duration};
use tokio::{sync::mpsc, task, time::sleep};
use tracing::{info, error, debug};
use reqwest::{Client, StatusCode}; 
use serde::Deserialize;

// --- Configuration ---

const EXTERNAL_ENDPOINT: &str = "https://your-object-store-api/list-objects";
const POLLING_INTERVAL: Duration = Duration::from_secs(5);

// Define a simple structure for the listing, though it will only be used
// if the ETag changes.
#[derive(Debug, Deserialize)]
struct ObjectListing(Vec<String>);


/// Watcher for an external resource using ETags for efficient change detection.
/// It polls the endpoint using the If-None-Match header, and only processes
/// the response if the ETag has changed (Status 200).
pub async fn start_external_watcher_etag(
    http_client: Client,
    tx: mpsc::Sender<()>,
) -> Result<(), anyhow::Error> {
    
    // Store the ETag from the last successful request
    let mut last_etag: Option<String> = None;

    info!("Starting ETag-based external resource watcher for: {}", EXTERNAL_ENDPOINT);

    task::spawn(async move {
        loop {
            // Wait for the polling interval
            sleep(POLLING_INTERVAL).await;

            debug!("Polling external endpoint with ETag: {:?}", last_etag);

            match poll_external_resource_etag(&http_client, last_etag.as_deref()).await {
                
                // 1. Change Detected (New ETag and new data)
                Some(PollResult::Changed { new_etag, listing_data }) => {
                    info!("External Resource Change Detected! New ETag: {}", new_etag);
                    debug!("New data received (first 5 objects): {:?}", &listing_data.0.iter().take(5).collect::<Vec<_>>());
                    
                    last_etag = Some(new_etag);

                    // Send signal to the controller/reconciler
                    if let Err(_) = tx.send(()).await {
                        error!("Failed to send change signal, receiver likely dropped.");
                        // Exit the loop if the channel is closed
                        break; 
                    }
                },

                // 2. No Change (Same ETag, Status 304 Not Modified)
                Some(PollResult::NotModified) => {
                    debug!("No change detected (Status 304).");
                },
                
                // 3. Error during Poll
                None => {
                    error!("External Watcher Poll Failed (see logs above). Retrying...");
                    // last_etag is not updated, keeping the old one for the next attempt
                }
            }
        }
        
        info!("ETag resource watcher task finished.");
    });

    Ok(())
}


// --- Helper Logic for Polling ---

/// Enum to represent the three possible outcomes of the conditional GET
enum PollResult {
    Changed { new_etag: String, listing_data: ObjectListing },
    NotModified,
}

/// Fetches the resource conditionally using the ETag.
async fn poll_external_resource_etag(client: &Client, etag: Option<&str>) -> Option<PollResult> {
    let mut request = client.get(EXTERNAL_ENDPOINT);
    
    // 1. Add the conditional header if we have a previous ETag
    if let Some(tag) = etag {
        request = request.header("If-None-Match", tag);
    }
    
    let response = match request.send().await {
        Ok(res) => res,
        Err(e) => {
            error!("HTTP request failed: {}", e);
            return None;
        }
    };

    // 2. Check the response status
    match response.status() {
        // Status 200 OK: Content has changed, process the body and ETag
        StatusCode::OK => {
            // Get the new ETag from the response headers
            let new_etag = match response.headers().get("ETag") {
                Some(tag_value) => tag_value.to_str().unwrap_or_default().to_string(),
                None => {
                    error!("Successful response (200) but no ETag header found. Cannot track state.");
                    return None;
                }
            };

            // Deserialize the body (the listing data)
            let listing: ObjectListing = match response.json().await {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to deserialize response body: {}", e);
                    return None;
                }
            };

            Some(PollResult::Changed { new_etag, listing_data: listing })
        },

        // Status 304 Not Modified: Content is the same, no body to process
        StatusCode::NOT_MODIFIED => {
            Some(PollResult::NotModified)
        },

        // Other statuses (4xx, 5xx): Error
        status => {
            error!("Endpoint returned non-success/non-304 status: {}", status);
            // Optionally log the body here for more debug info
            None
        }
    }
}