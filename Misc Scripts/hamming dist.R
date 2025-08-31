#edge stability
library(mgm)
library(dplyr)

# Function to compute edge stability
edge_stability <- function(tvvar_model) {
  edges <- tvvar_model$wadj  # Extract adjacency matrices over time
  if (is.list(edges)) {
    edges_array <- simplify2array(edges)  # Convert list to array (nodes x nodes x time)
    edge_variability <- apply(edges_array, c(1, 2), sd, na.rm = TRUE)  # Compute SD across time
    mean_weights <- apply(edges_array, c(1, 2), mean, na.rm = TRUE)  # Mean edge weight
    coef_var <- edge_variability / (mean_weights + 1e-10)  # Avoid division by zero
    
    return(list(sd_edges = edge_variability, cv_edges = coef_var))
  } else {
    stop("tvvar_model$wadj is not a list, check model output.")
  }
}

# Example for one participant
stability_results <- edge_stability(tvvar_model)
print(stability_results$sd_edges)  # Standard deviation of edge weights
print(stability_results$cv_edges)  # Coefficient of variation


#hamming distance: number of edge differences
# Function to compute Hamming distance
hamming_distance <- function(stat_model, tvvar_model) {
  stat_edges <- stat_model$wadj  # Stationary adjacency matrix
  tvvar_edges <- simplify2array(tvvar_model$wadj)  # Time-varying matrices
  
  if (is.list(tvvar_model$wadj)) {
    avg_tvvar_edges <- apply(tvvar_edges, c(1, 2), mean, na.rm = TRUE)  # Average across time
    hamming_dist <- sum(abs((stat_edges > 0) - (avg_tvvar_edges > 0)))  # Binary edge differences
    return(hamming_dist)
  } else {
    stop("tvvar_model$wadj is not a list, check model output.")
  }
}

# Example for one participant
hamming_dist_value <- hamming_distance(stat_model, tvvar_model)
print(hamming_dist_value)
