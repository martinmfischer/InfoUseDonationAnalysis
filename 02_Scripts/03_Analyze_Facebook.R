# ------------------------------------------------------------
# Facebook Explorative Analysis Script
# (Time Series & Structural EDA)
# ------------------------------------------------------------


#### Clean Env
rm(list = ls())
gc()

#### Packages
if(!require("pacman", character.only=TRUE)) install.packages("pacman")
pacman::p_load(
  tidyverse,
  lubridate,
  scales
)

# 
# 
# 
# 
# ### data displaying ###
# 
# # Liste der Facebook-Tibbles
# fb_tibbles <- list(
#   comments = "01_Data/facebook_comments_tibble.rds",
#   likes    = "01_Data/facebook_likes_tibble.rds",
#   follows  = "01_Data/facebook_follows_tibble.rds",
#   pages    = "01_Data/facebook_followed_pages_tibble.rds"
# )
# 
# # Funktion: load & head
# map(fb_tibbles, ~ {
#   df <- readRDS(.x)
#   message("\n--- ", names(fb_tibbles)[which(fb_tibbles == .x)], " ---")
#   print(head(df))
# })
# 
# 


#### Load Facebook Tibbles
fb_comments <- readRDS("01_Data/facebook_comments_tibble.rds")
fb_likes    <- readRDS("01_Data/facebook_likes_tibble.rds")
fb_follows  <- readRDS("01_Data/facebook_follows_tibble.rds")
fb_pages    <- readRDS("01_Data/facebook_followed_pages_tibble.rds")

#### Helper to safely extract character
safe_chr <- function(x) {
  if (is.null(x)) return(NA_character_)
  if (is.list(x)) x <- unlist(x, recursive = TRUE, use.names = FALSE)
  x <- as.character(x[1])
  if (is.na(x) || x == "") NA_character_ else x
}

#### Standardize columns and combine all interactions
comments_df <- fb_comments %>%
  transmute(
    participant,
    interaction = "comment",
    target_raw  = coalesce(safe_chr(Comment), safe_chr(Action)),
    timestamp   = safe_chr(Date)
  )

likes_df <- fb_likes %>%
  transmute(
    participant,
    interaction = "like",
    target_raw  = coalesce(safe_chr(Action), safe_chr(Reaction)),
    timestamp   = safe_chr(Date)
  )

follows_df <- fb_follows %>%
  transmute(
    participant,
    interaction = "follow",
    target_raw  = coalesce(safe_chr(name), safe_chr(title)),
    timestamp   = safe_chr(timestamp)
  )

pages_df <- fb_pages %>%
  transmute(
    participant,
    interaction = "page_follow",
    target_raw  = coalesce(safe_chr(name), safe_chr(title)),
    timestamp   = safe_chr(timestamp)
  )

facebook_all <- bind_rows(comments_df, likes_df, follows_df, pages_df) %>%
  mutate(
    timestamp = ymd_hms(timestamp, quiet = TRUE),
    date      = as_date(timestamp),
    week      = floor_date(date, "week"),
    month     = floor_date(date, "month")
  )

# ------------------------------
# 1. Aktivität pro Tag
# ------------------------------
activity_daily <- facebook_all %>%
  filter(!is.na(date)) %>%
  count(date, interaction)

ggplot(activity_daily, aes(date, n, color = interaction)) +
  geom_line(linewidth = 0.8, alpha = 0.8) +
  scale_y_continuous(trans = "sqrt") +
  labs(
    title = "Facebook daily activity by interaction type",
    x = NULL,
    y = "Interactions (sqrt scaled)",
    color = "Interaction"
  ) +
  theme_minimal()

# ------------------------------
# 2. Aktivität pro Teilnehmer (Woche)
# ------------------------------
activity_week <- facebook_all %>%
  filter(!is.na(week)) %>%
  count(participant, week, interaction)

ggplot(activity_week, aes(week, n, color = interaction)) +
  geom_line(alpha = 0.7) +
  facet_wrap(~ participant, scales = "free_y") +
  labs(
    title = "Weekly Facebook activity per participant",
    x = NULL, y = "Interactions"
  ) +
  theme_minimal()

# ------------------------------
# 3. Interaktionsmix über Zeit (Monat)
# ------------------------------
interaction_share <- facebook_all %>%
  filter(!is.na(month)) %>%
  count(month, interaction) %>%
  group_by(month) %>%
  mutate(share = n / sum(n))

ggplot(interaction_share, aes(month, share, fill = interaction)) +
  geom_area(alpha = 0.8) +
  scale_y_continuous(labels = percent_format()) +
  labs(
    title = "Interaction type share over time",
    y = "Share", x = NULL, fill = "Interaction"
  ) +
  theme_minimal()

# ------------------------------
# 4. Target Diversität pro Woche pro Teilnehmer
# ------------------------------
target_diversity <- facebook_all %>%
  filter(!is.na(week), !is.na(target_raw)) %>%
  group_by(participant, week) %>%
  summarise(
    n_interactions = n(),
    n_targets      = n_distinct(target_raw),
    .groups = "drop"
  )

ggplot(target_diversity, aes(week, n_targets / n_interactions)) +
  geom_line() +
  facet_wrap(~ participant, scales = "free_y") +
  labs(
    title = "Relative target diversity per week",
    y = "Targets / Interactions", x = NULL
  ) +
  theme_minimal()

# ------------------------------
# 5. Top Targets pro Teilnehmer
# ------------------------------
top_targets <- facebook_all %>%
  filter(!is.na(target_raw)) %>%
  count(participant, target_raw, sort = TRUE) %>%
  group_by(participant) %>%
  mutate(rank = row_number()) %>%
  filter(rank <= 10)

ggplot(top_targets, aes(reorder(target_raw, n), n)) +
  geom_col() +
  coord_flip() +
  facet_wrap(~ participant, scales = "free_y") +
  labs(
    title = "Top 10 targets per participant",
    x = "Target", y = "Interactions"
  ) +
  theme_minimal()

# ------------------------------
# 6. Long-Tail Plot
# ------------------------------
long_tail <- facebook_all %>%
  filter(!is.na(target_raw)) %>%
  count(target_raw) %>%
  arrange(desc(n)) %>%
  mutate(rank = row_number())

ggplot(long_tail, aes(rank, n)) +
  geom_point(alpha = 0.6) +
  scale_x_log10() + scale_y_log10() +
  labs(title = "Long-tail distribution of targets", x = "Rank (log)", y = "Interactions (log)") +
  theme_minimal()

# ------------------------------
# 7. Save processed data
# ------------------------------
# saveRDS(facebook_all, "02_Analysis/facebook_all_long.rds")
# saveRDS(activity_daily, "02_Analysis/facebook_activity_daily.rds")
# saveRDS(activity_week, "02_Analysis/facebook_activity_weekly.rds")
# saveRDS(target_diversity, "02_Analysis/facebook_target_diversity.rds")
# saveRDS(top_targets, "02_Analysis/facebook_top_targets.rds")
# saveRDS(long_tail, "02_Analysis/facebook_long_tail.rds")