use super::*;

fn page0_with_change_counter(counter: u32, fill: u8) -> Vec<u8> {
    let mut page = vec![fill; 4096];
    page[24..28].copy_from_slice(&counter.to_be_bytes());
    page
}

#[test]
fn replay_cursor_accepts_manifest_ahead_when_page0_matches() {
    let page0 = page0_with_change_counter(3, 0x11);

    validate_replay_cursor_page0(3, 7, &page0, Some(&page0))
        .expect("matching page0 lets direct replay cursor stay ahead of SQLite header counter");
}

#[test]
fn replay_cursor_accepts_zero_cache_counter_when_manifest_cursor_is_ahead() {
    let page0 = page0_with_change_counter(0, 0x11);

    validate_replay_cursor_page0(0, 7, &page0, Some(&page0))
        .expect("direct replay cursor may be valid before SQLite header counter advances");
}

#[test]
fn replay_cursor_rejects_lagging_counter_with_different_page0() {
    let cache_page0 = page0_with_change_counter(3, 0x11);
    let manifest_page0 = page0_with_change_counter(3, 0x22);

    let err = validate_replay_cursor_page0(3, 7, &cache_page0, Some(&manifest_page0))
        .expect_err("different page0 with lagging header counter is reset/copy drift");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn replay_cursor_accepts_cache_counter_at_or_above_manifest() {
    let cache_page0 = page0_with_change_counter(9, 0x11);
    let manifest_page0 = page0_with_change_counter(3, 0x22);

    validate_replay_cursor_page0(9, 7, &cache_page0, Some(&manifest_page0))
        .expect("normal local progress can have a newer cache header");
}

// =========================================================================
// Coordinate Math (tests for group_id, local_idx_in_group, group_start_page
// defined in this file)
// =========================================================================

#[test]
fn test_group_id_calculation() {
    let ppg = 2048u32;
    assert_eq!(group_id(0, ppg), 0);
    assert_eq!(group_id(2047, ppg), 0);
    assert_eq!(group_id(2048, ppg), 1);
    assert_eq!(group_id(4095, ppg), 1);
    assert_eq!(group_id(4096, ppg), 2);
}

#[test]
fn test_group_id_small_ppg() {
    assert_eq!(group_id(0, 1), 0);
    assert_eq!(group_id(1, 1), 1);
    assert_eq!(group_id(99, 1), 99);
    assert_eq!(group_id(0, 4), 0);
    assert_eq!(group_id(3, 4), 0);
    assert_eq!(group_id(4, 4), 1);
}

#[test]
fn test_local_idx_calculation() {
    let ppg = 2048u32;
    assert_eq!(local_idx_in_group(0, ppg), 0);
    assert_eq!(local_idx_in_group(2047, ppg), 2047);
    assert_eq!(local_idx_in_group(2048, ppg), 0);
    assert_eq!(local_idx_in_group(2049, ppg), 1);
}

#[test]
fn test_local_idx_small_ppg() {
    assert_eq!(local_idx_in_group(0, 1), 0);
    assert_eq!(local_idx_in_group(1, 1), 0);
    assert_eq!(local_idx_in_group(5, 3), 2);
    assert_eq!(local_idx_in_group(6, 3), 0);
}

#[test]
fn test_group_start_page() {
    let ppg = 2048u32;
    assert_eq!(group_start_page(0, ppg), 0);
    assert_eq!(group_start_page(1, ppg), 2048);
    assert_eq!(group_start_page(5, ppg), 10240);
}

#[test]
fn test_group_start_page_small_ppg() {
    assert_eq!(group_start_page(0, 1), 0);
    assert_eq!(group_start_page(3, 1), 3);
    assert_eq!(group_start_page(2, 4), 8);
}

#[test]
fn test_coordinate_math_roundtrip() {
    // For any page, group_start_page(group_id(p)) + local_idx(p) == p
    let ppg = 2048u32;
    for p in [0u64, 1, 2047, 2048, 2049, 4095, 4096, 100_000] {
        let gid = group_id(p, ppg);
        let idx = local_idx_in_group(p, ppg);
        let reconstructed = group_start_page(gid, ppg) + idx as u64;
        assert_eq!(reconstructed, p, "roundtrip failed for page {}", p);
    }
}
