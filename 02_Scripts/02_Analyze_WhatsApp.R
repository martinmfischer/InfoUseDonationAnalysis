# ------------------------------
# Analysis Script for WhatsApp
# Robust two-stage fuzzy matching
# ------------------------------

#### Clean Environment ####
rm(list = ls())
gc()

#### Load / Install Packages ####
if (!require("pacman", character.only = TRUE)) install.packages("pacman")
pacman::p_load(
  tidyverse,
  readxl,
  stringr,
  fuzzyjoin,
  urltools,
  vegan,
  stringi
)

# ------------------------------
# Load Public Speaker Database
# ------------------------------

db <- read_excel("05_Public_speaker_Database/data/DBOES_2024_12_komplett.xlsx") %>%
  as_tibble()

normalize_name <- function(x) {
  x %>%
    str_to_lower() %>%
    str_replace_all("[^a-z0-9üäö ]", " ") %>%
    str_squish()
}

db <- db %>%
  mutate(Name_clean = normalize_name(Name))

db_long <- db %>%
  select(KomplettID, Name, Kategorie, Typ, contains("URL"), Name_clean) %>%
  pivot_longer(
    cols = contains("URL"),
    names_to = "platform",
    values_to = "url"
  ) %>%
  filter(!is.na(url), url != "existiert nicht") %>%
  mutate(
    domain = url_parse(url)$domain %>% str_to_lower(),
    path   = url_parse(url)$path
  )

# ------------------------------
# Load WhatsApp data
# ------------------------------

whatsapp_tbl <- readRDS("01_Data/whatsapp_data_tibble.rds")

all_links <- whatsapp_tbl %>%
  unnest(messages) %>%
  filter(!is.na(link))

# ------------------------------
# Detect platform
# ------------------------------

all_links <- all_links %>%
  mutate(
    social_platform = case_when(
      str_detect(domain, "facebook.com")  ~ "facebook",
      str_detect(domain, "x.com")         ~ "x",
      str_detect(domain, "twitter.com")   ~ "twitter",
      str_detect(domain, "instagram.com") ~ "instagram",
      str_detect(domain, "tiktok.com")    ~ "tiktok",
      str_detect(domain, "youtube.com")   ~ "youtube",
      str_detect(domain, "youtu.be")      ~ "youtube_short",
      TRUE                                ~ "none"
    )
  )

# ------------------------------
# Extract account / handle
# ------------------------------

all_links <- all_links %>%
  mutate(
    account = case_when(
      social_platform == "facebook"      ~ str_remove(link, "https?://(www\\.)?facebook\\.com/"),
      social_platform == "x"             ~ str_remove(link, "https?://(www\\.)?x\\.com/"),
      social_platform == "twitter"       ~ str_extract(link, "(?<=twitter\\.com/)[^/]+"),
      social_platform == "instagram"     ~ str_remove(link, "https?://(www\\.)?instagram\\.com/|/$"),
      social_platform == "tiktok"        ~ str_extract(link, "(?<=tiktok\\.com/@)[^/?]+|(?<=vm\\.tiktok\\.com/)[^/?]+"),
      social_platform == "youtube"       ~ str_remove(link, "https?://(www\\.)?youtube\\.com/"),
      social_platform == "youtube_short" ~ str_remove(link, "https?://youtu\\.be/"),
      TRUE                               ~ NA_character_
    ),
    account = if_else(
      is.na(account),
      suffix_extract(domain)$domain,
      account
    )
  )

# ------------------------------
# Robust two-stage matching
# ------------------------------

normalize_handle <- function(x) {
  x %>%
    str_to_lower() %>%
    str_remove("^@") %>%
    str_remove("/.*$") %>%
    str_replace_all("[^a-z0-9]", "")
}

all_links_clean <- all_links %>%
  filter(!is.na(account)) %>% 
  filter(stri_length(account) > 2) %>% 
  mutate(
    account_norm = normalize_handle(account)
  )

db_match_clean <- db_long %>%
  select(KomplettID, Name, Kategorie, Typ, path, Name_clean) %>%
  filter(!is.na(path)) %>%
  filter(stri_length(path) > 2) %>% 
  mutate(
    path_norm = normalize_handle(path)
  )

# ---- Stage 1: exact ----
exact_matches <- inner_join(
  all_links_clean,
  db_match_clean,
  by = c("account_norm" = "path_norm"),
  relationship = "many-to-many"
) %>%
  group_by(link) %>%
  slice(1) %>%        # or slice_min(KomplettID)
  ungroup() %>%
  mutate(dist = 0, rel_dist = 0, match_type = "exact")


unmatched_links <- anti_join(
  all_links_clean,
  exact_matches,
  by = "link"
)

exact_matches_2 <- inner_join(
  unmatched_links,
  db_match_clean,
  by = c("account_norm" = "Name_clean"),
  relationship = "many-to-many"
) %>%
  group_by(link) %>%
  slice(1) %>%        # or slice_min(KomplettID)
  ungroup() %>%
  mutate(dist = 0, rel_dist = 0, match_type = "exact")






# ---- Stage 2: fuzzy ----
unmatched_links <- anti_join(
  all_links_clean,
  exact_matches,
  by = "link"
)

fuzzy_matches <- stringdist_left_join(
  unmatched_links,
  db_match_clean,
  by = c("account_norm" = "path_norm"),
  distance_col = "dist"
) %>%
  mutate(
    rel_dist = dist / pmax(nchar(account_norm), nchar(path_norm), 1),
    match_type = "fuzzy"
  ) %>%
  filter(rel_dist < 0.15) %>%
  group_by(link) %>%
  slice_min(rel_dist, n = 1, with_ties = FALSE) %>%
  ungroup()

# ---- Combine & ORDER by DBÖS ID (TODO #2) ----
fuzzy_links <- bind_rows(exact_matches, exact_matches_2, fuzzy_matches) %>%
  select(
    participant, chat_name, date, link, domain, social_platform,
    account, account_norm,
    KomplettID, Name, Kategorie, Typ,
    match_type, dist, rel_dist
  ) %>%
  arrange(KomplettID, date)

# ------------------------------
# Ambiguity diagnostics
# ------------------------------

ambiguity_check <- stringdist_left_join(
  all_links_clean,
  db_match_clean,
  by = c("account_norm" = "path_norm"),
  distance_col = "dist"
) %>%
  mutate(rel_dist = dist / pmax(nchar(account_norm), nchar(path_norm), 1)) %>%
  filter(rel_dist < 0.15) %>%
  group_by(link) %>%
  summarise(
    n_candidates = n(),
    min_rel_dist = min(rel_dist),
    .groups = "drop"
  )

fuzzy_links <- fuzzy_links %>%
  left_join(ambiguity_check, by = "link") %>%
  mutate(ambiguous = n_candidates > 1)

# ------------------------------
# DBÖS-centric aggregation (NEW, TODO-driven)
# ------------------------------

dbos_summary <- fuzzy_links %>%
  group_by(KomplettID, Name, Kategorie, Typ) %>%
  summarise(
    n_links = n(),
    n_participants = n_distinct(participant),
    share_ambiguous = mean(ambiguous, na.rm = TRUE),
    mean_rel_dist = mean(rel_dist),
    .groups = "drop"
  ) %>%
  arrange(KomplettID)


# ------------------------------
# Build table of unmatched links
# ------------------------------

unmatched_links_tbl <- all_links_clean %>%
  filter(!link %in% fuzzy_links$link) %>%    # only links that were NOT matched
  select(participant, chat_name, date, link, domain, social_platform, account)

# Optionally: order by participant, chat, date
unmatched_links_tbl <- unmatched_links_tbl %>%
  arrange(participant, chat_name, date)
# ------------------------------
# Descriptives
# ------------------------------

wa_exposure <- all_links %>%
  mutate(is_dbos = link %in% fuzzy_links$link) %>%
  group_by(participant) %>%
  summarise(
    total_links = n(),
    dbos_links = sum(is_dbos),
    share_dbos = dbos_links / total_links
  )

wa_diversity <- fuzzy_links %>%
  count(participant, KomplettID) %>%
  group_by(participant) %>%
  summarise(
    shannon = diversity(n),
    n_dbos_links = sum(n),
    n_sources = n()
  )

# ------------------------------
# Global summary table
# ------------------------------

summary_df <- tibble(
  total_whatsapp_links = nrow(all_links),
  candidate_links = nrow(all_links_clean),
  matched_links = nrow(fuzzy_links),
  retention_rate = matched_links / candidate_links,
  median_rel_dist = median(fuzzy_links$rel_dist),
  p90_rel_dist = quantile(fuzzy_links$rel_dist, 0.9),
  max_rel_dist = max(fuzzy_links$rel_dist),
  ambiguous_links = sum(fuzzy_links$ambiguous),
  share_ambiguous = mean(fuzzy_links$ambiguous),
  participants_total = n_distinct(all_links$participant),
  participants_with_dbos_links = n_distinct(fuzzy_links$participant),
  mean_share_dbos_links = mean(wa_exposure$share_dbos),
  median_share_dbos_links = median(wa_exposure$share_dbos),
  mean_shannon = mean(wa_diversity$shannon),
  median_shannon = median(wa_diversity$shannon)
)

summary_long_df <- summary_df %>%
  pivot_longer(everything(), names_to = "metric", values_to = "value") %>%
  arrange(metric)

print(summary_long_df, n = Inf)
