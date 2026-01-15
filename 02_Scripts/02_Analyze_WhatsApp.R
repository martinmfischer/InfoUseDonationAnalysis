# ------------------------------
# Analysis Script for WhatsApp
# ------------------------------


###Todo - retain DBÖS ID in final link list! Order by that as well!

#### Clean Env
rm(list = ls())
gc()

#### Load/Install Packages ####
if(!require("pacman", character.only=TRUE)) install.packages("pacman")
pacman::p_load(
  "tidyverse",
  "purrr",
  "dplyr",
  "readxl",
  "stringr",
  "fuzzyjoin",
  "urltools",
  "tidyr",
  "progress",
  "vegan"
)





#### Load Public Speaker Database for filtering ########

db <- read_excel("05_Public_speaker_Database/data/DBOES_2024_12_komplett.xlsx") %>% as_tibble()

social_patterns <- c(facebook = "facebook.com",
                     x = "x.com",
                     instagram = "instagram.com",
                     tiktok = "tiktok.com",
                     twitter = "twitter.com",
                     youtube = "youtube.com",
                     youtube_short = "youtu.be")



normalize_name <- function(x) {
  x %>%
    stringr::str_to_lower() %>%
    stringr::str_replace_all("[^a-z0-9 ]", " ") %>%
    stringr::str_replace_all("\\b(tv|magazin|zeitung|nachrichten|online)\\b", "") %>%
    stringr::str_squish()
}

db <- db %>%
  mutate(Name_clean = normalize_name(Name))

db_long <- db %>%
  select(KomplettID, Name, Kategorie, Typ, contains("URL")) %>%
  pivot_longer(
    cols = contains("URL"),
    names_to = "platform",
    values_to = "url"
  ) %>%
  filter(!is.na(url), url != "existiert nicht") %>%
  mutate(
    domain = url_parse(url)$domain %>% str_to_lower(),
    domain_suffix = suffix_extract(domain)$domain,
    path = url_parse(url)$path
  )


db_names <- db %>%
  select(KomplettID, Name) %>%
  mutate(Name = str_to_lower(Name))



#### main loop ####
#load data

data_path <- "01_Data/whatsapp_data_tibble.rds"   
whatsapp_tbl <- readRDS(data_path)


# flatten nested link tibble

all_links <- whatsapp_tbl %>%
  unnest(messages) %>%   # jede Nachricht wird eine Zeile
  filter(!is.na(link))




all_links <- all_links %>%
  mutate(
    social_platform = case_when(
      str_detect(domain, "facebook.com") ~ "facebook",
      str_detect(domain, "x.com") ~ "x",
      str_detect(domain, "instagram.com") ~ "instagram",
      str_detect(domain, "tiktok.com") ~ "tiktok",
      str_detect(domain, "twitter.com") ~ "twitter",
      str_detect(domain, "youtube.com") ~ "youtube",
      str_detect(domain, "youtu.be") ~ "youtube_short",
      TRUE ~ "none"
    )
  )



all_links <- all_links %>%
  mutate(
    account = case_when(
      social_platform == "facebook"       ~ str_remove(link, "https?://(www\\.)?facebook\\.com/"),
      social_platform == "x"              ~ str_remove(link, "https?://(www\\.)?x\\.com/"),
      social_platform == "twitter"        ~ str_extract(link, "(?<=twitter\\.com/)[^/]+"),
      social_platform == "instagram"      ~ str_remove(link, "https?://(www\\.)?instagram\\.com/|/$"),
      social_platform == "tiktok"         ~ str_extract(link, "(?<=tiktok\\.com/@)[^/?]+|(?<=vm\\.tiktok\\.com/)[^/?]+"),
      social_platform == "youtube"        ~ str_remove(link, "https?://(www\\.)?youtube\\.com/"),
      social_platform == "youtube_short"  ~ str_remove(link, "https?://youtu\\.be/"),
      TRUE ~ NA_character_
    ),
    # Für alle Nicht-Socials: Domain-Stamm extrahieren
    account = if_else(
      is.na(account),
      suffix_extract(domain)$domain,
      account
    )
  )

# --- Prepare db_long for fuzzy join ---
db_match <- db_long %>%
  select(KomplettID, Name, Kategorie, Typ, path) %>%
  mutate(path = str_to_lower(path))

# 1. Filter NAs aus account und path
all_links_clean <- all_links %>%
  filter(!is.na(account)) %>%
  mutate(account = str_to_lower(account))



db_match_clean <- db_long %>%
  select(KomplettID, Name, Kategorie, Typ, path) %>%
  filter(!is.na(path)) %>%
  mutate(path = str_to_lower(path))

# 2. Fuzzy join
fuzzy_links <- stringdist_left_join(
  all_links_clean,
  db_match_clean,
  by = c("account" = "path"),
  distance_col = "dist",
  max_dist = Inf
) %>%
  mutate(
    rel_dist = dist / pmax(nchar(account), nchar(path), 1)
  ) %>%
  filter(rel_dist < 0.15) %>%
  group_by(link) %>%
  slice_min(rel_dist, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(participant, chat_name, date, link, domain, social_platform, account,
         KomplettID, Name, Kategorie, Typ, dist, rel_dist)




# Ausgangsdaten
n_total_links <- nrow(all_links)
n_social_links <- sum(all_links$social_platform != "none", na.rm = TRUE)
n_candidates <- nrow(all_links_clean)
n_matched <- nrow(fuzzy_links)

tibble(
  total_links = n_total_links,
  candidate_links = n_candidates,
  matched_links = n_matched,
  retention_rate = n_matched / n_candidates
)






platform_retention <- all_links_clean %>%
  mutate(is_matched = link %in% fuzzy_links$link) %>%
  group_by(social_platform) %>%
  summarise(
    total = n(),
    matched = sum(is_matched),
    retention = matched / total
  ) %>%
  arrange(desc(retention))

platform_retention

fuzzy_links %>%
  summarise(
    median_rel_dist = median(rel_dist),
    p90 = quantile(rel_dist, 0.9),
    max = max(rel_dist)
  )


ggplot(fuzzy_links, aes(rel_dist)) +
  geom_histogram(bins = 40) +
  geom_vline(xintercept = 0.15, linetype = "dashed") +
  labs(
    title = "Distribution der relativen Fuzzy-Distanzen",
    x = "Relative Distanz",
    y = "Anzahl Matches"
  ) +
  theme_minimal()


ambiguity_check <- stringdist_left_join(
  all_links_clean,
  db_match_clean,
  by = c("account" = "path"),
  distance_col = "dist"
) %>%
  mutate(rel_dist = dist / pmax(nchar(account), nchar(path), 1)) %>%
  filter(rel_dist < 0.15) %>%
  group_by(link) %>%
  summarise(
    n_candidates = n(),
    min_dist = min(rel_dist)
  )

ambiguity_check %>%
  summarise(
    ambiguous_links = sum(n_candidates > 1),
    share_ambiguous = mean(n_candidates > 1)
  )


### Analyses

wa_exposure <- all_links %>%
  mutate(is_dbos = link %in% fuzzy_links$link) %>%
  group_by(participant) %>%
  summarise(
    total_links = n(),
    dbos_links = sum(is_dbos),
    share_dbos = dbos_links / total_links
  )


ggplot(wa_exposure, aes(share_dbos)) +
  geom_density(fill = "grey70", alpha = 0.6) +
  labs(
    title = "Anteil DBÖS-Links an allen WhatsApp-Links",
    x = "Anteil DBÖS",
    y = "Dichte"
  ) +
  theme_minimal()



wa_diversity <- fuzzy_links %>%
  count(participant, KomplettID) %>%
  group_by(participant) %>%
  summarise(
    shannon = diversity(n, index = "shannon"),
    n_dbos_links = sum(n),
    n_sources = n()
  )

ggplot(wa_diversity, aes(shannon)) +
  geom_histogram(bins = 30) +
  labs(
    title = "Diversität öffentlich relevanter Quellen (WhatsApp)",
    x = "Shannon-Index",
    y = "Teilnehmer"
  ) +
  theme_minimal()


wa_timeseries <- fuzzy_links %>%
  mutate(date = as.Date(date)) %>%
  count(date)

ggplot(wa_timeseries, aes(date, n)) +
  geom_line() +
  labs(
    title = "Zeitverlauf geteilter DBÖS-Links (WhatsApp)",
    x = "Datum",
    y = "Anzahl Links"
  ) +
  theme_minimal()



# ------------------------------
# GLOBAL SUMMARY TABLE
# ------------------------------

summary_df <- tibble(
  
  ## --- Datenbasis ---
  total_whatsapp_links = nrow(all_links),
  candidate_links = nrow(all_links_clean),
  matched_links = nrow(fuzzy_links),
  retention_rate = matched_links / candidate_links,
  
  ## --- Distanzdiagnostik ---
  median_rel_dist = median(fuzzy_links$rel_dist, na.rm = TRUE),
  p90_rel_dist = quantile(fuzzy_links$rel_dist, 0.9, na.rm = TRUE),
  max_rel_dist = max(fuzzy_links$rel_dist, na.rm = TRUE),
  
  ## --- Ambiguität ---
  ambiguous_links = sum(ambiguity_check$n_candidates > 1, na.rm = TRUE),
  share_ambiguous = mean(ambiguity_check$n_candidates > 1, na.rm = TRUE),
  
  ## --- WhatsApp Exposure ---
  participants_total = n_distinct(all_links$participant),
  participants_with_dbos_links = n_distinct(fuzzy_links$participant),
  mean_share_dbos_links = mean(wa_exposure$share_dbos, na.rm = TRUE),
  median_share_dbos_links = median(wa_exposure$share_dbos, na.rm = TRUE),
  
  ## --- WhatsApp Diversity ---
  mean_shannon = mean(wa_diversity$shannon, na.rm = TRUE),
  median_shannon = median(wa_diversity$shannon, na.rm = TRUE),
  
)

# ---- Ausgabe im Chat / Console ----
print(summary_df, width = Inf)

# ------------------------------
# PIVOT SUMMARY TABLE
# ------------------------------

summary_long_df <- summary_df %>%
  pivot_longer(
    cols = everything(),
    names_to = "metric",
    values_to = "value"
  ) %>%
  arrange(metric)

# Ausgabe im Chat / Console
print(summary_long_df, n = Inf)

