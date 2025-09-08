# ------------------------------
# Analysis Script
# ------------------------------


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


db_long <- db %>%
  select(KomplettID, Name, contains("URL")) %>%
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
  social_patterns <- c("facebook.com", "instagram.com", "x.com", "tiktok.com")
  # str_detect works rowwise via map_lgl
  map_lgl(domains, ~ any(str_detect(.x, social_patterns)))
}



link_in_database <- function(link, max_dist = 5) {
  ## test env
  
  #link = "www.aachexcvbcxvbnerzeitung.de/zeitungslink"
  #max_dist = 3
  
  # --- Extract path from the link
  path_to_seek <- link %>%
    url_parse() %>%
    .$path %>%
    str_to_lower()
  
  domain_to_seek <- link %>%
    url_parse() %>%
    .$domain %>%
    str_to_lower()
  
  # --- Prepare DB in long format (all social URLs in one column)
  db_long <- db %>%
    select(KomplettID, Name, contains("URL")) %>%
    tidyr::pivot_longer(cols = contains("URL"),
                        names_to = "platform",
                        values_to = "url") %>%
    filter(!is.na(url), url != "existiert nicht") %>%
    mutate(domain = url_parse(url)$domain %>% str_to_lower()) %>%
    mutate(domain_suffix = suffix_extract(domain)$domain,
           path = url_parse(url)$path)
  
  
  
  if (is_social_domain(domain_to_seek)) {
    message("we got a social domain!")
    
    if (is.na(path_to_seek))
      return(FALSE)
    
    
  } else {
    message("we got a web domain!")
    
    suf_ex <- domain_to_seek %>% suffix_extract()
    path_to_seek <- suf_ex$domain ## the name of our publication is now in the domain, not the path. Overriding this.
    
    
  }
  
  # --- 1. Exact path match
  exact_match <- db_long %>%
    filter(path == path_to_seek)
  
  if (nrow(exact_match) > 0)
    return(TRUE)
  
  
  # --- 2. Fuzzy match between path and Name (for newspaper websites)
  fuzzy <- stringdist_left_join(
    tibble(path = path_to_seek),
    db %>% select(KomplettID, Name) %>% mutate(Name = str_to_lower(Name)),
    by = c("path" = "Name"),
    max_dist = max_dist
  )
  
  
  match <- !all(is.na(fuzzy$KomplettID))
  
  return(match)
  
}


link_in_database_vec <- function(links, max_dist = 5) {
  
  parsed <- url_parse(links)
  domains <- str_to_lower(parsed$domain)
  paths <- str_to_lower(parsed$path)
  
  # Social domain check (vectorized)
  is_social <- is_social_domain(domains)
  
  # --- Vectorized loop with map_lgl (still preserves all logic)
  map_lgl(seq_along(links), function(i) {
    domain_to_seek <- domains[i]
    path_to_seek <- paths[i]
    
    if (is_social[i]) {
      # Social domain logic
      if (is.na(path_to_seek)) return(FALSE)
    } else {
      # Web domain logic: override path with domain
      suf_ex <- suffix_extract(domain_to_seek)
      path_to_seek <- suf_ex$domain
    }
    
    # 1. Exact path match in DB
    exact_match <- db_long %>% filter(path == path_to_seek)
    if (nrow(exact_match) > 0) return(TRUE)
    
    # 2. Fuzzy match for web domains (social domains skip fuzzy)
    
    if (is.na(path_to_seek)) return(FALSE)
    if (!is_social[i]) {
      fuzzy <- stringdist_left_join(
        tibble(path = path_to_seek),
        db_names,
        by = c("path" = "Name"),
        max_dist = max_dist
      )
      match <- !all(is.na(fuzzy$KomplettID))
      return(match)
    }
    
    # If social domain and no exact match
    return(FALSE)
  })
}


filter_links <- function(links, max_dist = 5, chunk_size = 100) {
  
  n <- nrow(links)
  results <- logical(n)
  pb <- progress_bar$new(total = n, format = "[:bar] :percent (:current/:total)")
  
  # Process in chunks
  for (start_idx in seq(1, n, by = chunk_size)) {
    idx <- start_idx:min(start_idx + chunk_size - 1, n)
    results[idx] <- link_in_database_vec(links$link[idx], max_dist = max_dist)
    pb$tick(length(idx))
  }
  
  all_links_filtered <- links[results, ]
  
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

all_links <- filter_links(all_links)



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
