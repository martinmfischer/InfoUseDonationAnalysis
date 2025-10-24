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
  "progress"
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


#### helper functions ####





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
