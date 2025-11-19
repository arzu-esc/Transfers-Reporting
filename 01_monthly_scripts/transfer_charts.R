### --- PREP SECTION FOR SANKEY CHARTS --- ###

# Get current financial year
current_fy <- transfers %>%
  filter(stat_shortcut == "M57A") %>%
  summarise(max(FinYear, na.rm = TRUE)) %>%
  pull()

# Get the latest month’s stat_date
latest_month_date <- transfers %>%
  filter(stat_shortcut == "M57A", FinYear == current_fy) %>%
  summarise(latest = max(stat_date, na.rm = TRUE)) %>%
  pull()

# All retailer-to-retailer movements for latest month
retailer_movements_latest <- transfers %>%
  filter(
    stat_shortcut == "M57A",
    stat_date == latest_month_date,
    !is.na(FRMP_Retailer_name),
    !is.na(NEWFRMP_Retailer_name)
  ) %>%
  group_by(
    FRMP_Retailer_name,
    NEWFRMP_Retailer_name,
    FRMP_Retailer_size,
    NEWFRMP_Retailer_size
  ) %>%
  summarise(transfers = sum(stat_value, na.rm = TRUE), .groups = "drop")

# Retailer colours by size
retailer_size_colors <- c(
  "Small"  = colour_palette[["blue_main"]],
  "Medium" = colour_palette[["gray_2"]],
  "Large"  = colour_palette[["gray_3"]]
)

fallback_color <- colour_palette[["gray_4"]]  # For size = NA

assign_node_color <- function(size) {
  ifelse(size %in% names(retailer_size_colors),
         retailer_size_colors[size],
         fallback_color)
}

create_sankey_top_out <- function() {
  
  ## --- Top 5 losing retailers (OUT) ---
  top_out_retailers <- retailer_movements_latest %>%
    group_by(FRMP_Retailer_name, FRMP_Retailer_size) %>%
    summarise(total_out = sum(transfers), .groups = "drop") %>%
    arrange(desc(total_out)) %>%
    slice(1:5)
  
  data_filtered <- retailer_movements_latest %>%
    filter(FRMP_Retailer_name %in% top_out_retailers$FRMP_Retailer_name)
  
  ## SORT left nodes by transfers descending
  from_retailers <- top_out_retailers$FRMP_Retailer_name
  
  ## Right nodes (all destinations)
  to_retailers <- data_filtered %>%
    group_by(NEWFRMP_Retailer_name) %>%
    summarise(total = sum(transfers)) %>%
    arrange(desc(total)) %>%
    pull(NEWFRMP_Retailer_name)
  
  n_from <- length(from_retailers)
  n_to   <- length(to_retailers)
  
  ## LEFT NODES (FROM)
  from_node_map <- data.frame(
    retailer = from_retailers,
    index    = 0:(n_from - 1)
  ) %>%
    left_join(top_out_retailers %>% 
                rename(retailer = FRMP_Retailer_name, size = FRMP_Retailer_size),
              by = "retailer") %>%
    mutate(
      label = retailer,
      color = assign_node_color(size)
    )
  
  ## RIGHT NODES (TO)
  retailer_sizes_to <- data_filtered %>%
    distinct(NEWFRMP_Retailer_name, NEWFRMP_Retailer_size) %>%
    rename(retailer = NEWFRMP_Retailer_name, size = NEWFRMP_Retailer_size)
  
  to_node_map <- data.frame(
    retailer = to_retailers,
    index    = n_from:(n_from + n_to - 1)
  ) %>%
    left_join(retailer_sizes_to, by = "retailer") %>%
    mutate(
      label = retailer,
      color = assign_node_color(size)
    )
  
  ## COMBINE NODES
  all_nodes <- bind_rows(from_node_map, to_node_map)
  
  ## LINKS (proportional opacity + tooltips)
  max_flow <- max(data_filtered$transfers, na.rm = TRUE)
  
  link_data <- data_filtered %>%
    left_join(from_node_map %>% select(retailer, source = index),
              by = c("FRMP_Retailer_name" = "retailer")) %>%
    left_join(to_node_map %>% select(retailer, target = index),
              by = c("NEWFRMP_Retailer_name" = "retailer")) %>%
    mutate(
      opacity = pmax(0.2, transfers / max_flow),
      link_color = paste0("rgba(35,96,146,", opacity, ")"),
      hover = paste0(
        "<b>", transfers, " transfers</b><br>",
        "From: ", FRMP_Retailer_name, "<br>",
        "To: ", NEWFRMP_Retailer_name
      )
    )
  
  ## PLOT
  plot_ly(
    type = "sankey",
    orientation = "h",
    node = list(
      label = all_nodes$label,
      color = all_nodes$color,
      pad = 15, thickness = 20,
      line = list(color = "white", width = 0.5),
      hovertemplate = paste(
        "<b>%{label}</b><br>",
        "Size: %{customdata}<extra></extra>"
      ),
      customdata = all_nodes$size
    ),
    link = list(
      source = link_data$source,
      target = link_data$target,
      value  = link_data$transfers,
      color  = link_data$link_color,
      hovertemplate = "%{customdata}<extra></extra>",
      customdata = link_data$hover
    )
  ) %>%
    layout(
      title = list(
        text = paste("Top 5 Retailers Losing Customers –", format(latest_month_date, "%B %Y")),
        font = font_title
      ),
      font = font_main,
      margin = list(t = 80, b = 40, l = 150, r = 150)
    )
}

create_sankey_top_in <- function() {
  
  ## --- Top 5 gaining retailers (IN) ---
  top_in_retailers <- retailer_movements_latest %>%
    group_by(NEWFRMP_Retailer_name, NEWFRMP_Retailer_size) %>%
    summarise(total_in = sum(transfers), .groups = "drop") %>%
    arrange(desc(total_in)) %>%
    slice(1:5)
  
  data_filtered <- retailer_movements_latest %>%
    filter(NEWFRMP_Retailer_name %in% top_in_retailers$NEWFRMP_Retailer_name)
  
  ## SORT right nodes by volume
  to_retailers <- top_in_retailers$NEWFRMP_Retailer_name
  
  ## Sort left nodes by inbound volume
  from_retailers <- data_filtered %>%
    group_by(FRMP_Retailer_name) %>%
    summarise(total = sum(transfers)) %>%
    arrange(desc(total)) %>%
    pull(FRMP_Retailer_name)
  
  n_from <- length(from_retailers)
  n_to   <- length(to_retailers)
  
  ## LEFT NODES (FROM)
  retailer_sizes_from <- data_filtered %>%
    distinct(FRMP_Retailer_name, FRMP_Retailer_size) %>%
    rename(retailer = FRMP_Retailer_name, size = FRMP_Retailer_size)
  
  from_node_map <- data.frame(
    retailer = from_retailers,
    index    = 0:(n_from - 1)
  ) %>%
    left_join(retailer_sizes_from, by = "retailer") %>%
    mutate(
      label = retailer,
      color = assign_node_color(size)
    )
  
  ## RIGHT NODES (TO)
  to_node_map <- data.frame(
    retailer = to_retailers,
    index    = n_from:(n_from + n_to - 1)
  ) %>%
    left_join(top_in_retailers %>%
                rename(retailer = NEWFRMP_Retailer_name, size = NEWFRMP_Retailer_size),
              by = "retailer") %>%
    mutate(
      label = retailer,
      color = assign_node_color(size)
    )
  
  ## COMBINE NODES
  all_nodes <- bind_rows(from_node_map, to_node_map)
  
  ## LINKS (proportional opacity + tooltips)
  max_flow <- max(data_filtered$transfers, na.rm = TRUE)
  
  link_data <- data_filtered %>%
    left_join(from_node_map %>% select(retailer, source = index),
              by = c("FRMP_Retailer_name" = "retailer")) %>%
    left_join(to_node_map %>% select(retailer, target = index),
              by = c("NEWFRMP_Retailer_name" = "retailer")) %>%
    mutate(
      opacity = pmax(0.2, transfers / max_flow),
      link_color = paste0("rgba(35,96,146,", opacity, ")"),
      hover = paste0(
        "<b>", transfers, " transfers</b><br>",
        "From: ", FRMP_Retailer_name, "<br>",
        "To: ", NEWFRMP_Retailer_name
      )
    )
  
  ## PLOT
  plot_ly(
    type = "sankey",
    orientation = "h",
    node = list(
      label = all_nodes$label,
      color = all_nodes$color,
      pad = 15, thickness = 20,
      line = list(color = "white", width = 0.5),
      hovertemplate = paste(
        "<b>%{label}</b><br>",
        "Size: %{customdata}<extra></extra>"
      ),
      customdata = all_nodes$size
    ),
    link = list(
      source = link_data$source,
      target = link_data$target,
      value  = link_data$transfers,
      color  = link_data$link_color,
      hovertemplate = "%{customdata}<extra></extra>",
      customdata = link_data$hover
    )
  ) %>%
    layout(
      title = list(
        text = paste("Top 5 Retailers Gaining Customers –", format(latest_month_date, "%B %Y")),
        font = font_title
      ),
      font = font_main,
      margin = list(t = 80, b = 40, l = 150, r = 150)
    )
}
