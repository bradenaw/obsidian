use std::cmp::Ordering;

pub fn binary_search_by_idx<K: Ord, F: Fn(usize) -> K>(
    n: usize,
    k: K,
    f: F,
) -> Result<usize, usize> {
    let mut lower = 0;
    let mut upper = n;
    while lower < upper {
        let mid = (lower + upper) / 2;
        let at_mid = f(mid);
        match k.cmp(&at_mid) {
            Ordering::Equal => return Ok(mid),
            Ordering::Less => upper = mid,
            Ordering::Greater => lower = mid + 1,
        }
    }
    Err(lower)
}
