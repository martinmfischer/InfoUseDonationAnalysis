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
  "urltools"
)





#### Load Public Speaker Database

db <- read_excel("05_Public_speaker_Database/data/DBOES_2024_12_komplett.xlsx") %>% as_tibble()



print_attributes <- function(x) {
  atts <- attributes(x)
  msg <- paste(names(atts), "=", sapply(atts, toString), collapse = "; ")
  message("Attributes -> ", msg)
}


is_social_domain <- function(domain_to_seek) {
  platforms <- c("tiktok.com", "facebook.com", "instagram.com", "x.com")
  any(str_detect(domain_to_seek, platforms))
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
  
  
  
  if(is_social_domain(domain_to_seek)) {
    message("we got a social domain!")
    
    if(is.na(path_to_seek)) return(NULL)

    
  } else {
    message("we got a web domain!")
    
    suf_ex <- domain_to_seek %>% suffix_extract()
    path_to_seek <- suf_ex$domain ## the name of our publication is now in the domain, not the path. Overriding this.
    
    
  }
  
  # --- 1. Exact path match
  exact_match <- db_long %>%
    filter(path == path_to_seek)
  
  if (nrow(exact_match) > 0) return(exact_match)
  
  
  # --- 2. Fuzzy match between path and Name (for newspaper websites)
  fuzzy <- stringdist_left_join(
    tibble(path = path_to_seek),
    db %>% select(KomplettID, Name) %>% mutate(Name = str_to_lower(Name)),
    by = c("path" = "Name"),
    max_dist = max_dist
  )
  
  
  no_match <- all(is.na(fuzzy$KomplettID))
  
  if (!no_match) return(fuzzy)
  
  return(NULL)
  
}

filter_links <- function(links) {
  
  links_filtered <- links %>% filter(link_in_database(link))
  
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
