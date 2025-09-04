# ------------------------------
# WhatsApp Links Extraction Script
# ------------------------------

# 1. Load the previously saved RDS file
data_path <- "01_Data/parsed_data.rds"   # <-- adjust the path
whatsapp_data <- readRDS(data_path)

# 2. Initialize empty list to store results
all_links <- list()
############# ALL IS BROKEN
# 3. Loop over each participant
for (participant in whatsapp_data) {
  
  # Extract participant ID
  participant_id <- attr(participant$whatsapp[[1]], "participant")
  
  # Loop over top chats (if multiple)
  for (chat_name in names(participant$whatsapp[[1]])) {
    if (grepl("whatsapp_links_with_context", chat_name)) next # skip list names that are not links
    
    chat <- participant$whatsapp[[1]][[chat_name]]
    
    if (!is.null(chat$whatsapp_links_with_context)) {
      for (link_item in chat$whatsapp_links_with_context) {
        all_links[[length(all_links) + 1]] <- data.frame(
          participant = participant_id,
          link = link_item$link,
          domain = link_item$domain,
          stringsAsFactors = FALSE
        )
      }
    }
  }
}

# 4. Combine all into one dataframe
links_df <- do.call(rbind, all_links)

# 5. Count occurrences per domain per participant
library(dplyr)
domain_counts <- links_df %>%
  group_by(participant, domain) %>%
  summarise(count = n(), .groups = "drop") %>%
  arrange(participant, desc(count))

# 6. Save results
saveRDS(links_df, "whatsapp_links_combined.rds")
saveRDS(domain_counts, "whatsapp_domain_counts.rds")
write.csv(domain_counts, "whatsapp_domain_counts.csv", row.names = FALSE)

# 7. Optional: view top domains overall
top_domains <- domain_counts %>%
  group_by(domain) %>%
  summarise(total_count = sum(count)) %>%
  arrange(desc(total_count))
print(head(top_domains, 20))
