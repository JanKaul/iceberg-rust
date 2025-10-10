use datafusion_expr::Join;

pub trait SelectivityEstimate {
    fn selectivity(_join: &Join) -> f64 {
        0.1
    }
}
