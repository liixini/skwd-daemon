use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::Serialize;
use steamworks::{
    AppIDs, AppId, Client, ItemState, PublishedFileId, SingleClient, UGCQueryType,
    UGCStatisticType, UGCType,
};
use tokio::sync::{mpsc, oneshot};

const WALLPAPER_ENGINE_APP_ID: u32 = 431_960;
const INIT_APP_ID: u32 = WALLPAPER_ENGINE_APP_ID;

#[derive(Serialize, Clone, Debug)]
pub struct WorkshopItem {
    pub id: String,
    pub title: String,
    pub description: String,
    pub preview_url: String,
    pub creator: String,
    pub votes_up: u64,
    pub votes_down: u64,
    pub subscriptions: u64,
    pub favorites: u64,
    pub views: u64,
    pub file_size: u64,
    pub tags: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum SortMode {
    Trend,
    MostVoted,
    Recent,
    Subscriptions,
    MostFavorited,
}

impl SortMode {
    pub fn from_str(s: &str) -> Self {
        match s {
            "votes" | "ranked" => SortMode::MostVoted,
            "recent" | "new"   => SortMode::Recent,
            "subs"  | "subscriptions" => SortMode::Subscriptions,
            "favorites" | "fav" => SortMode::MostFavorited,
            _ => SortMode::Trend,
        }
    }

    fn to_ugc(self) -> UGCQueryType {
        match self {
            SortMode::Trend         => UGCQueryType::RankedByTrend,
            SortMode::MostVoted     => UGCQueryType::RankedByVote,
            SortMode::Recent        => UGCQueryType::RankedByPublicationDate,
            SortMode::Subscriptions => UGCQueryType::RankedByTotalUniqueSubscriptions,
            SortMode::MostFavorited => UGCQueryType::FavoritedByFriendsRankedByPublicationDate,
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct SearchFilters {
    pub required_tags: Vec<String>,
    pub excluded_tags: Vec<String>,
    pub match_any_required: bool,
    pub trend_days: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DownloadProgress {
    pub current: u64,
    pub total: u64,
    pub state: u32,
}

pub fn is_pending(state_bits: u32) -> bool {
    ItemState::from_bits_truncate(state_bits).contains(ItemState::DOWNLOAD_PENDING)
}

pub fn is_downloading(state_bits: u32) -> bool {
    ItemState::from_bits_truncate(state_bits).contains(ItemState::DOWNLOADING)
}

#[derive(Clone, Debug, Serialize)]
pub struct DownloadComplete {
    pub install_folder: String,
    pub size_on_disk: u64,
}

enum Command {
    Search {
        page: u32,
        search: String,
        sort: SortMode,
        filters: SearchFilters,
        reply: oneshot::Sender<Result<Vec<WorkshopItem>>>,
    },
    Download {
        id: u64,
        progress: mpsc::UnboundedSender<DownloadProgress>,
        reply: oneshot::Sender<Result<DownloadComplete>>,
    },
}

#[derive(Clone)]
pub struct WorkshopBrowser {
    tx: mpsc::Sender<Command>,
}

impl WorkshopBrowser {
    pub fn try_init() -> Option<Self> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(16);
        let (init_tx, init_rx) = std::sync::mpsc::channel::<bool>();

        std::thread::Builder::new()
            .name("steamworks-browser".into())
            .spawn(move || {
                let (client, single): (Client, SingleClient) = match Client::init_app(INIT_APP_ID) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::info!(
                            "steamworks init failed (Steam not running, or account doesn't own Wallpaper Engine): {e}"
                        );
                        let _ = init_tx.send(false);
                        return;
                    }
                };
                tracing::info!(
                    "steamworks browser initialised against Wallpaper Engine ({INIT_APP_ID}), Steam is running"
                );
                let _ = init_tx.send(true);

                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::warn!("steamworks worker rt build failed: {e}");
                        return;
                    }
                };

                rt.block_on(async move {
                    while let Some(cmd) = cmd_rx.recv().await {
                        match cmd {
                            Command::Search { page, search, sort, filters, reply } => {
                                let result = run_query(&client, &single, page, &search, sort, &filters).await;
                                let _ = reply.send(result);
                            }
                            Command::Download { id, progress, reply } => {
                                let result = run_download(&client, &single, id, progress).await;
                                let _ = reply.send(result);
                            }
                        }
                    }
                });
            })
            .ok()?;

        match init_rx.recv() {
            Ok(true) => Some(Self { tx: cmd_tx }),
            _ => None,
        }
    }

    pub async fn search(
        &self,
        page: u32,
        search: &str,
        sort: SortMode,
        filters: SearchFilters,
    ) -> Result<Vec<WorkshopItem>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Command::Search {
                page,
                search: search.to_string(),
                sort,
                filters,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow!("steamworks worker stopped"))?;
        reply_rx.await.map_err(|_| anyhow!("steamworks reply dropped"))?
    }

    pub async fn download(
        &self,
        id: u64,
        progress: mpsc::UnboundedSender<DownloadProgress>,
    ) -> Result<DownloadComplete> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Command::Download { id, progress, reply: reply_tx })
            .await
            .map_err(|_| anyhow!("steamworks worker stopped"))?;
        reply_rx.await.map_err(|_| anyhow!("steamworks reply dropped"))?
    }
}

async fn run_query(
    client: &Client,
    single: &SingleClient,
    page: u32,
    search: &str,
    sort: SortMode,
    filters: &SearchFilters,
) -> Result<Vec<WorkshopItem>> {
    let we = AppId(WALLPAPER_ENGINE_APP_ID);
    let page = page.max(1);

    let mut handle = client
        .ugc()
        .query_all(
            sort.to_ugc(),
            UGCType::Items,
            AppIDs::Both { creator: we, consumer: we },
            page,
        )
        .map_err(|e| anyhow!("steamworks query_all failed: {e:?}"))?
        .set_return_long_description(false)
        .set_return_metadata(true);

    if !search.is_empty() {
        handle = handle.set_search_text(search);
    }

    for tag in &filters.required_tags {
        if !tag.is_empty() {
            handle = handle.add_required_tag(tag);
        }
    }
    for tag in &filters.excluded_tags {
        if !tag.is_empty() {
            handle = handle.add_excluded_tag(tag);
        }
    }
    if filters.match_any_required {
        handle = handle.set_match_any_tag(true);
    }
    if let Some(days) = filters.trend_days
        && matches!(sort, SortMode::Trend)
    {
        handle = handle.set_ranked_by_trend_days(days);
    }

    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<Vec<WorkshopItem>>>(1);
    handle.fetch(move |result| {
        let converted: Result<Vec<WorkshopItem>> = match result {
            Err(e) => Err(anyhow!("steamworks fetch failed: {e:?}")),
            Ok(res) => {
                let mut out = Vec::with_capacity(res.returned_results() as usize);
                for i in 0..res.returned_results() {
                    let Some(d) = res.get(i) else { continue };
                    out.push(WorkshopItem {
                        id: d.published_file_id.0.to_string(),
                        title: d.title,
                        description: d.description,
                        preview_url: res.preview_url(i).unwrap_or_default(),
                        creator: d.owner.raw().to_string(),
                        votes_up: d.num_upvotes.into(),
                        votes_down: d.num_downvotes.into(),
                        subscriptions: res.statistic(i, UGCStatisticType::Subscriptions).unwrap_or(0),
                        favorites: res.statistic(i, UGCStatisticType::Favorites).unwrap_or(0),
                        views: res.statistic(i, UGCStatisticType::UniqueWebsiteViews).unwrap_or(0),
                        file_size: d.file_size.into(),
                        tags: d.tags,
                    });
                }
                Ok(out)
            }
        };
        let _ = tx.send(converted);
    });

    let timeout = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        single.run_callbacks();
        match rx.try_recv() {
            Ok(r) => return r,
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if std::time::Instant::now() > timeout {
                    return Err(anyhow!("steamworks fetch timed out after 20s"));
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!("steamworks callback dropped"));
            }
        }
    }
}

async fn run_download(
    client: &Client,
    single: &SingleClient,
    id: u64,
    progress: mpsc::UnboundedSender<DownloadProgress>,
) -> Result<DownloadComplete> {
    let item = PublishedFileId(id);

    let (sub_tx, sub_rx) = std::sync::mpsc::sync_channel::<std::result::Result<(), String>>(1);
    client.ugc().subscribe_item(item, move |r| {
        let _ = sub_tx.send(r.map_err(|e| format!("{e:?}")));
    });

    let sub_deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        single.run_callbacks();
        match sub_rx.try_recv() {
            Ok(Ok(())) => break,
            Ok(Err(e)) => return Err(anyhow!("SubscribeItem failed: {e}")),
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if std::time::Instant::now() > sub_deadline {
                    return Err(anyhow!("SubscribeItem timed out after 20s"));
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                return Err(anyhow!("SubscribeItem callback dropped"));
            }
        }
    }

    if !client.ugc().download_item(item, true) {
        return Err(anyhow!("DownloadItem rejected (Steam not running, or item invalid)"));
    }

    let download_deadline = std::time::Instant::now() + Duration::from_secs(60 * 30);
    let mut idle_since: Option<std::time::Instant> = None;
    let mut last_progress: (u64, u64) = (0, 0);

    loop {
        single.run_callbacks();

        let state = client.ugc().item_state(item);
        let state_bits = state.bits();
        let (current, total) = client.ugc().item_download_info(item).unwrap_or((0, 0));

        if (current, total) != last_progress {
            last_progress = (current, total);
            idle_since = None;
        }

        let _ = progress.send(DownloadProgress { current, total, state: state_bits });

        if state.contains(ItemState::INSTALLED) && !state.contains(ItemState::DOWNLOADING) {
            if let Some(info) = client.ugc().item_install_info(item) {
                return Ok(DownloadComplete {
                    install_folder: info.folder,
                    size_on_disk: info.size_on_disk,
                });
            }
            return Err(anyhow!("Steam reports INSTALLED but item_install_info returned None"));
        }

        let is_active = state.contains(ItemState::DOWNLOADING)
            || state.contains(ItemState::DOWNLOAD_PENDING)
            || total > 0;
        if !is_active {
            let now = std::time::Instant::now();
            let since = idle_since.get_or_insert(now);
            if now.duration_since(*since) > Duration::from_secs(15) {
                return Err(anyhow!(
                    "Steam did not start the download (state bits {state_bits}). Is the Wallpaper Engine account logged in?"
                ));
            }
        } else {
            idle_since = None;
        }

        if std::time::Instant::now() > download_deadline {
            return Err(anyhow!("download timed out after 30 minutes"));
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::SortMode;

    #[test]
    fn sort_mode_from_str_maps_aliases() {
        assert!(matches!(SortMode::from_str("votes"), SortMode::MostVoted));
        assert!(matches!(SortMode::from_str("ranked"), SortMode::MostVoted));
        assert!(matches!(SortMode::from_str("new"), SortMode::Recent));
        assert!(matches!(SortMode::from_str("subscriptions"), SortMode::Subscriptions));
        assert!(matches!(SortMode::from_str("fav"), SortMode::MostFavorited));
        assert!(matches!(SortMode::from_str("anything-else"), SortMode::Trend));
    }
}
