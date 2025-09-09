# ------------------------------
# Analysis Script
# ------------------------------


###Todo - retain DBÖS ID in final link list! Order by that as well!

#### Clean Env
rm(list = ls())
gc()

#### Load/Install Packages
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





#### Load Public Speaker Database

db <- read_excel("05_Public_speaker_Database/data/DBOES_2024_12_komplett.xlsx") %>% as_tibble()


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





print_attributes <- function(x) {
  atts <- attributes(x)
  msg <- paste(names(atts), "=", sapply(atts, toString), collapse = "; ")
  message("Attributes -> ", msg)
}


is_social_domain <- function(domains) {
  social_patterns <- c("facebook.com", "instagram.com", "x.com", "tiktok.com", "twitter.com")
  # str_detect works rowwise via map_lgl
  map_lgl(domains, ~ any(str_detect(.x, social_patterns)))
}




link_in_database_vec <- function(links, max_dist = 2) {
  
  
  
  parsed <- url_parse(links)
  domains <- str_to_lower(parsed$domain)
  paths <- str_to_lower(parsed$path)
  
  # Social domain check (vectorized)
  is_social <- is_social_domain(domains)
  
  # Ergebnisliste vorbereiten
  results <- map(seq_along(links), function(i) {
    message("DEBUG: Searching for link: ", links[i])
    domain_to_seek <- domains[i]
    path_to_seek <- paths[i]
    
    if (is_social[i]) {
      # Social domain logic
      message("Is social: yes! for path_to_seek=", path_to_seek)
      if (is.na(path_to_seek)) return(NULL)
    } else {
      message("Is social: no! for path_to_seek=", path_to_seek)
      
      # Web domain logic: override path with domain
      suf_ex <- suffix_extract(domain_to_seek)
      path_to_seek <- suf_ex$domain
    }
    
    # 1. Exact path match in DB
    exact_match <- db_long %>% filter(path == path_to_seek)
    if (nrow(exact_match) > 0) {
      message("Exact match with path_to_seek=", path_to_seek)
      print(exact_match)
      return(exact_match %>% 
               select(KomplettID, Name, Kategorie, Typ) %>% 
               slice(1) %>% 
               mutate(match_type = "exact"))
    }
    
    # 2. Fuzzy match for web domains (social domains skip fuzzy)
    if (is.na(path_to_seek)) return(NULL)
    if (!is_social[i]) {
      
      message("Fuzzy matching path_to_seek=", path_to_seek, " with max dist=")
      fuzzy <- stringdist_left_join(
        tibble(path = path_to_seek),
        db %>% select(KomplettID, Name, Name_clean, Kategorie, Typ) %>% mutate(Name = str_to_lower(Name)),
        by = c("path" = "Name_clean"),
        max_dist = max_dist
      )
      best <- fuzzy %>% filter(!is.na(KomplettID)) %>% slice(1)
      message("We got the following fuzzy result:")
      print(fuzzy)
      if (nrow(best) > 0) {
        return(best %>% select(KomplettID, Name, Kategorie, Typ) %>% mutate(match_type = "fuzzy"))
      }
    }
    
    # Kein Treffer
    return(NULL)
  })
  
  return(results)
}


filter_links <- function(links, max_dist = 2, chunk_size = 100) {
  
  n <- nrow(links)
  matches <- vector("list", n)
  pb <- progress_bar$new(total = n, format = "[:bar] :percent (:current/:total)")
  message("DEBUG - filter_links function, max dist is =", max_dist)
  # Process in chunks
  for (start_idx in seq(1, n, by = chunk_size)) {
    idx <- start_idx:min(start_idx + chunk_size - 1, n)
    res <- link_in_database_vec(links$link[idx], max_dist = max_dist)
    matches[idx] <- res
    pb$tick(length(idx))
  }
  
  # Nur Zeilen behalten, die Treffer haben
  has_match <- map_lgl(matches, ~ !is.null(.x))
  
  all_links_filtered <- links[has_match, ] %>%
    bind_cols(bind_rows(matches[has_match]))
  
  n_filtered <- nrow(all_links_filtered)
  message("We filtered ", n - n_filtered, " links. Remaining links: ", n_filtered)
  
  return(all_links_filtered)
}



# 1. Load the previously saved RDS file
data_path <- "01_Data/parsed_data.rds"   
data <- readRDS(data_path)

# 2. Initialize empty tibble to store all links
all_links <- tibble(participant = character(),
                    domain = character(),
                    link = character())

# 3. Loop over each participant, Whatsapp analysis
for (participant in data) {
  
  if (is.null(attr(participant, "has_whatsapp")) || attr(participant, "has_whatsapp") == FALSE) next
  
  # Extract participant ID
  participant_id <- attr(participant, "participant")
  
  chat_list <- participant$whatsapp
  
  message("participant_id is ", participant_id)
  print_attributes(participant)
  
  # Loop over top chats
  for (chat_name in names(chat_list[[1]])) {
    if (!grepl("whatsapp_links_with_context", chat_name)) next
    
    chat <- chat_list[[1]][[chat_name]]
    message("Number of links in chat ", chat_name, " is ", length(chat))
    
    # Extract domains and links
    chat_df <- tibble(
      participant = participant_id,
      domain = sapply(chat, function(x) x$domain),
      link = sapply(chat, function(x) x$link)
    )
    
    # Append to master table
    all_links <- bind_rows(all_links, chat_df)
    
  }
}

#### Filter to dbös!

all_links <-  all_links  %>% filter_links(max_dist = 2, chunk_size = 100)
### currently still inadequate! for instance, see Spiegel vs Spiegel TV etc


####

# 4. Aggregate across all participants
domain_summary <- all_links %>%
  group_by(domain) %>%
  summarise(
    total_links = n(),                           # total number of links
    participants_with_link = n_distinct(participant)  # number of distinct participants
    
  ) %>%
  arrange(desc(total_links))



# 5. Optional: Count per participant per domain
domain_counts <- all_links %>%
  group_by(participant, domain) %>%
  summarise(count = n(), .groups = "drop") %>%
  arrange(participant, desc(count))

# 6. Save results
saveRDS(all_links, "whatsapp_links_combined.rds")
saveRDS(domain_counts, "whatsapp_domain_counts.rds")
write.csv(domain_counts, "whatsapp_domain_counts.csv", row.names = FALSE)
write.csv(domain_summary, "whatsapp_domain_summary.csv", row.names = FALSE)

# 7. Optional: view top domains overall
print(head(domain_summary, 20))
