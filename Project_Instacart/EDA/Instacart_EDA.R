library(readr)
library(dplyr)

# read data

products <- read_csv("C:/Users/poohb/Documents/Gatech Tech/CSE6242 Data & Visual Analytics/Project/Instacart data/products.csv")
aisles <- read_csv("C:/Users/poohb/Documents/Gatech Tech/CSE6242 Data & Visual Analytics/Project/Instacart data/aisles.csv")
departments <- read_csv("C:/Users/poohb/Documents/Gatech Tech/CSE6242 Data & Visual Analytics/Project/Instacart data/departments.csv")
orders <- read_csv("C:/Users/poohb/Documents/Gatech Tech/CSE6242 Data & Visual Analytics/Project/Instacart data/orders.csv")
order_product_prior <- read_csv("C:/Users/poohb/Documents/Gatech Tech/CSE6242 Data & Visual Analytics/Project/Instacart data/order_products__prior.csv")
order_product_train <- read_csv("C:/Users/poohb/Documents/Gatech Tech/CSE6242 Data & Visual Analytics/Project/Instacart data/order_products__train.csv")


# merge products, aisles, and departments
products <- merge(products, aisles, by="aisle_id")
products <- merge(products, departments, by="department_id")

# join prior and train datasets
prior_train <- rbind(order_product_prior, order_product_train)

# join prior_train with orders data
orders <- merge(prior_train, orders, by="order_id")

# join orders with products
df <- merge(orders, products, by="product_id")

# reorder columns
df <- select(df, order_id, user_id, product_id, product_name, order_number, order_dow, reordered, order_hour_of_day, days_since_prior_order, department_id, department, aisle_id, aisle, eval_set)

# export as csv
write_csv(df, "instacart.csv")
# write_csv(df, "C:/Users/poohb/Documents/Gatech Tech/CSE6242 Data & Visual Analytics/Project/Instacart data/instacart.csv")


#################################### EDA #######################################
head(instacart)

# Summary Statistics
summary(instacart)  

# Load libraries
library(ggplot2)
library(plyr)
library(dplyr)
library(scales)


# Histogram for order_hour_of_day
ggplot(instacart, aes(x = as.numeric(order_hour_of_day))) +
  geom_histogram(binwidth = 1, fill = "blue", color = "black") +
  labs(title = "Histogram of Order Hour of Day", x = "Hour of Day", y = "Frequency")

# Box Plot
df_summary <- instacart %>%
  group_by(department) %>%
  summarize(median_order_hour = median(as.numeric(order_hour_of_day, na.rm = TRUE)))

ggplot(df_summary, aes(x = department, y = median_order_hour)) +
  geom_boxplot(fill = "lightblue", color = "blue") +
  labs(title = "Box Plot of Median Order Hour by Department", x = "Department", y = "Median Order Hour")

# Bar Chart
ggplot(instacart, aes(x = department)) +
  geom_bar(fill = "green", color = "black") +
  labs(title = "Bar Chart of Departments", x = "Department", y = "Count")


## Visualize the order_hour_of_day

# Calculate Number of Orders and Percentage of Orders
Orders_everyhour <- instacart %>%
  group_by(order_hour_of_day) %>%
  summarise(Number_of_Orders = n()) %>%
  mutate(Percentage_of_orders = (Number_of_Orders * 100 / sum(Number_of_Orders)))

# Sort the data by order hour
Orders_everyhour <- Orders_everyhour[order(Orders_everyhour$order_hour_of_day),]

# Set custom colors for the clock plot
custom_colors <- rainbow(nrow(Orders_everyhour), s = 1, v = 1, start = 0, end = max(1, nrow(Orders_everyhour) - 1) / nrow(Orders_everyhour), alpha = 0.5)

# Create the clock plot
x <- Orders_everyhour$Percentage_of_orders
clock.plot <- function (x, col, ...) {
  n <- length(x)
  if (min(x) < 0) x <- x - min(x)
  if (max(x) > 1) x <- x / max(x)
  if (is.null(names(x))) names(x) <- 0:(n - 1)
  m <- 1.05
  plot(0, type = 'n', xlim = c(-m, m), ylim = c(-m, m), axes = F, xlab = '', ylab = '', ...)
  a <- pi/2 - 2 * pi/200 * 0:200
  polygon(cos(a), sin(a))
  v <- 0.02
  a <- pi/2 - 2 * pi/n * 0:n
  segments((1 + v) * cos(a), (1 + v) * sin(a), (1 - v) * cos(a), (1 - v) * sin(a))
  segments(cos(a), sin(a), 0, 0, col = 'light grey', lty = 3)
  ca <- -2 * pi / n * (0:50) / 50
  for (i in 1:n) {
    a <- pi/2 - 2 * pi/n * (i - 1)
    b <- pi/2 - 2 * pi/n * i
    polygon(c(0, x[i] * cos(a + ca), 0), c(0, x[i] * sin(a + ca), 0), col = col[i])
    v <- .1
    text((1 + v) * cos(a), (1 + v) * sin(a), names(x)[i])
  }
}

# Create the clock plot with custom colors
clock.plot(x, col = custom_colors, main = "Peak Ordering Hours")


## Days Since Prior Order Analysis

# Calculate the frequency of days since prior order
Reordering_Gap <- count(instacart, 'days_since_prior_order') %>%
  arrange(desc(freq)) %>%
  mutate(Percent_orders = round(freq * 100 / nrow(instacart), 2))

# Create a histogram to visualize the gap between orders
Reordering_Gap_plot <- ggplot(Reordering_Gap, aes(x = days_since_prior_order, y = Percent_orders)) +
  geom_bar(stat = "identity", fill = "skyblue", color = "blue") +
  scale_x_continuous(name = "Days Since Prior Order", breaks = seq(0, 30, 1)) +
  scale_y_continuous(name = "Percentage of Orders", breaks = seq(0, 100, 10)) +
  ggtitle("Gap between Two Orders") +
  labs(x = "Days Since Prior Order", y = "Percentage") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.border = element_blank(),
        axis.line = element_line(color = "black"),
        plot.title = element_text(hjust = 0.5, size = 16, face = "bold"),
        axis.title = element_text(size = 12, face = "bold"),
        axis.text = element_text(size = 10))

print(Reordering_Gap_plot)


## Visualize the top 20 aisles

# Group the data by aisle
Number_of_Product_each_Aisle <- instacart %>%
  group_by(aisle) %>%
  count() %>%
  arrange(desc(n))

# Select the top 20 aisles
Top_20 <- head(Number_of_Product_each_Aisle, n = 20)

# Create a bar chart
ggplot(Top_20, aes(x = reorder(aisle, freq), y = freq)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  labs(title = "Top 20 Aisles by Frequency", y = "Frequency", x = "Aisle") +
  scale_y_continuous(labels = comma) +
  theme_minimal() +
  theme(axis.text.y = element_text(size = 10)) +
  coord_flip() +
  scale_x_discrete(position = "top")


## Visualize the top 20 products

# Group the data by product_name
Number_of_Product_each_Product <- instacart %>%
  group_by(product_name) %>%
  count() %>%
  arrange(desc(n))

# Select the top 20 products
Top_20_Products <- head(Number_of_Product_each_Product, n = 20)

# Create a bar chart for the top 20 products
ggplot(Top_20_Products, aes(x = reorder(product_name, n), y = n)) +
  geom_bar(stat = "identity", fill = "skyblue") +
  labs(title = "Top 20 Products by Frequency", y = "Frequency", x = "Product Name") +
  scale_y_continuous(labels = comma) +
  theme_minimal() +
  theme(axis.text.y = element_text(size = 6)) +
  coord_flip() +
  scale_x_discrete(position = "top")


## Treemap

install.packages("plotly")
library(dplyr)
library(treemap)

# Extract unique department information
unique_departments <- unique(instacart$department)

# Create a new dataframe for departments
departments <- data.frame(department = unique_departments)

# Extract unique aisle information
unique_aisles <- unique(instacart$aisle)

# Create a new dataframe for aisles
aisles <- data.frame(aisle = unique_aisles)

# Extract unique product information
unique_products <- unique(instacart$product_name)

# Create a new dataframe for products
products <- data.frame(product_name = unique_products)

# Group and summarize data
tmp1 <- instacart %>%
  group_by(aisle, department) %>%
  summarise(count = n()) %>%
  ungroup() %>%
  left_join(aisles, by = "aisle") %>%
  left_join(departments, by = "department") %>%
  group_by(department, aisle) %>%
  summarise(count2 = n())

# Create the treemap
treemap(tmp1, index = c("department", "aisle"), vSize = "count2", title = "", palette = "Set3", border.col = "#FFFFFF")

