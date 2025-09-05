# ------------------------------
# Analysis Script
# ------------------------------

pacman::p_load(
  "tidyverse",
  "purrr",
  "dplyr"
)

print_attributes <- function(x) {
  atts <- attributes(x)
  msg <- paste(names(atts), "=", sapply(atts, toString), collapse = "; ")
  message("Attributes -> ", msg)
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
