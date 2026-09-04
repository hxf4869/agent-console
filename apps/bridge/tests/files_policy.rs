//! 预览/下载策略与单 Range 测试(§22.3、§22.4)。

use bridge::files::{
    check_preview, classify, open_slice, parse_single_range, preview_limits, Classification,
    FileGrantManager, GrantAction, GrantActions, GrantSource, NotPreviewableReason, TransferMode,
    CACHE_CONTROL, MAX_DOWNLOAD_BYTES, MAX_IMAGE_PREVIEW_BYTES, MAX_PDF_PREVIEW_BYTES,
    MAX_TEXT_PREVIEW_BYTES, NOSNIFF,
};

fn write(path: &std::path::Path, content: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

fn verified(root: &std::path::Path, rel: &str) -> bridge::files::VerifiedFile {
    let mgr = FileGrantManager::new();
    let h = mgr
        .issue(
            "d",
            "s",
            root,
            std::path::Path::new(rel),
            GrantActions::all(),
            GrantSource::UserOpened,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
    mgr.resolve_for_read("d", "s", &h.token, GrantAction::Preview)
        .unwrap()
}

async fn read_body(slice: bridge::files::FileSlice) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut body = slice.body;
    let mut out = Vec::new();
    body.read_to_end(&mut out).await?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// MIME 嗅探(§22.3:以实际探测为准,不只信扩展名)
// ---------------------------------------------------------------------------

#[test]
fn classify_sniffs_allowed_types() {
    let png = b"\x89PNG\r\n\x1a\nrest";
    assert_eq!(classify(png), Classification::InlineImage("image/png"));
    assert_eq!(
        classify(b"\xff\xd8\xff\xe0junk"),
        Classification::InlineImage("image/jpeg")
    );
    assert_eq!(
        classify(b"GIF89a...."),
        Classification::InlineImage("image/gif")
    );
    assert_eq!(
        classify(b"RIFF\x24\x00\x00\x00WEBPVP8 "),
        Classification::InlineImage("image/webp")
    );
    assert_eq!(classify(b"%PDF-1.7 ..."), Classification::InlinePdf);
}

#[test]
fn html_and_svg_are_source_text_never_executed() {
    // 无扩展名参与:HTML/SVG 前缀按 UTF-8 文本处理 → text/plain 源码 inline。
    let html = b"<!DOCTYPE html><html><body><script>alert(1)</script></body></html>";
    assert_eq!(classify(html), Classification::InlineText);
    let svg = b"<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>";
    assert_eq!(classify(svg), Classification::InlineText);
    assert_eq!(
        Classification::InlineText.content_type(),
        "text/plain; charset=utf-8"
    );
    assert!(Classification::InlineText.is_inline());
}

#[test]
fn unknown_binary_is_attachment() {
    // 无效 UTF-8 且嗅探不出类型 → attachment
    let junk: Vec<u8> = (0..64).map(|i| (i * 37 + 200) as u8).collect();
    assert_eq!(
        classify(&junk),
        Classification::Attachment("application/octet-stream")
    );
}

#[test]
fn extension_does_not_deceive_sniffing() {
    // .png 名字但内容是文本:按文本处理(MIME 嗅探优先于扩展名)。
    let tmp = tempfile::tempdir().unwrap();
    write(&tmp.path().join("fake.png"), b"plain text not a png");
    let root = tmp.path();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let slice = open_slice(verified(root, "fake.png"), TransferMode::Preview, None)
            .await
            .unwrap();
        assert_eq!(slice.content_type, "text/plain; charset=utf-8");
    });
}

// ---------------------------------------------------------------------------
// 集中限制(§22.3:能力数据)
// ---------------------------------------------------------------------------

#[test]
fn limits_are_centralized_and_exposed() {
    let limits = preview_limits();
    assert_eq!(limits.text_preview_bytes, MAX_TEXT_PREVIEW_BYTES);
    assert_eq!(limits.text_preview_bytes, 2 * 1024 * 1024);
    assert_eq!(limits.image_preview_bytes, 25 * 1024 * 1024);
    assert_eq!(limits.pdf_preview_bytes, 100 * 1024 * 1024);
    assert_eq!(limits.download_bytes, 512 * 1024 * 1024);
    assert_eq!(limits.upload_bytes, 20 * 1024 * 1024);
    assert_eq!(MAX_IMAGE_PREVIEW_BYTES, 25 * 1024 * 1024);
    assert_eq!(MAX_PDF_PREVIEW_BYTES, 100 * 1024 * 1024);
    assert_eq!(MAX_DOWNLOAD_BYTES, 512 * 1024 * 1024);
    assert!(NOSNIFF);
    assert_eq!(CACHE_CONTROL, "private, no-store");
}

#[test]
fn preview_size_and_type_reasons() {
    // 超预览限:带 size 原因(允许下载,由上层决定)
    let r = check_preview(&Classification::InlineText, MAX_TEXT_PREVIEW_BYTES + 1);
    assert_eq!(
        r,
        Err(NotPreviewableReason::SizeLimit {
            size: MAX_TEXT_PREVIEW_BYTES + 1,
            limit: MAX_TEXT_PREVIEW_BYTES
        })
    );
    // 未超:通过
    assert_eq!(check_preview(&Classification::InlineText, 10), Ok(()));
    // PDF 超 100 MiB
    assert!(matches!(
        check_preview(&Classification::InlinePdf, MAX_PDF_PREVIEW_BYTES + 1),
        Err(NotPreviewableReason::SizeLimit { .. })
    ));
    // 图片超 25 MiB
    assert!(matches!(
        check_preview(
            &Classification::InlineImage("image/png"),
            MAX_IMAGE_PREVIEW_BYTES + 1
        ),
        Err(NotPreviewableReason::SizeLimit { .. })
    ));
    // 非 inline 类型:类型原因
    assert_eq!(
        check_preview(&Classification::Attachment("video/mp4"), 1),
        Err(NotPreviewableReason::TypeUnsupported)
    );
}

// ---------------------------------------------------------------------------
// 单 Range(§22.4:仅单 Range,无效/多段/越界稳定错误,不整文件回退)
// ---------------------------------------------------------------------------

#[test]
fn range_parsing_rules() {
    let total = 100u64;
    // 单段
    assert_eq!(
        parse_single_range("bytes=10-19", total).unwrap(),
        bridge::files::HttpRange {
            start: 10,
            end_inclusive: 19
        }
    );
    assert_eq!(parse_single_range("bytes=90-", total).unwrap().len(), 10);
    assert_eq!(
        parse_single_range("bytes=-5", total).unwrap(),
        bridge::files::HttpRange {
            start: 95,
            end_inclusive: 99
        }
    );
    // end 越界截断到 total-1
    assert_eq!(
        parse_single_range("bytes=95-1000", total).unwrap(),
        bridge::files::HttpRange {
            start: 95,
            end_inclusive: 99
        }
    );
    // suffix 覆盖整个文件
    assert_eq!(
        parse_single_range("bytes=-500", total).unwrap(),
        bridge::files::HttpRange {
            start: 0,
            end_inclusive: 99
        }
    );

    // 无效/多段/越界 → RANGE_INVALID
    for bad in [
        "bytes=0-1,3-4",   // 多段
        "bytes=5-2",       // end < start
        "bytes=100-",      // start 越界
        "bytes=1000-2000", // 完全越界
        "bytes=-0",        // 空后缀
        "bytes=abc",       // 语法错
        "items=0-5",       // 非 bytes 单位
        "",                // 空
    ] {
        assert!(
            parse_single_range(bad, total).is_err(),
            "range {bad:?} should be invalid"
        );
    }
    // 空文件上任何 range 不可满足
    assert!(parse_single_range("bytes=0-", 0).is_err());
    assert!(parse_single_range("bytes=-1", 0).is_err());
}

// ---------------------------------------------------------------------------
// open_slice 端到端(从 grant 复验到切片)
// ----------------------------------------------------------------------

#[tokio::test]
async fn open_slice_full_and_ranged() {
    let tmp = tempfile::tempdir().unwrap();
    let content: Vec<u8> = (0..100u32)
        .map(|i| b"0123456789"[i as usize % 10])
        .collect();
    write(&tmp.path().join("data.txt"), &content);

    // 整文件
    let slice = open_slice(
        verified(tmp.path(), "data.txt"),
        TransferMode::Download,
        None,
    )
    .await
    .unwrap();
    assert_eq!(slice.content_type, "text/plain; charset=utf-8");
    assert!(slice.inline);
    assert_eq!(slice.body_len, 100);
    assert_eq!(slice.total_len, 100);
    assert!(slice.content_range.is_none());
    assert_eq!(read_body(slice).await.unwrap(), content);

    // bytes=10-19
    let slice = open_slice(
        verified(tmp.path(), "data.txt"),
        TransferMode::Download,
        Some("bytes=10-19"),
    )
    .await
    .unwrap();
    assert_eq!(slice.body_len, 10);
    assert_eq!(
        slice.content_range_value().as_deref(),
        Some("bytes 10-19/100")
    );
    assert_eq!(read_body(slice).await.unwrap(), &content[10..20]);

    // bytes=-5 → 最后 5 字节
    let slice = open_slice(
        verified(tmp.path(), "data.txt"),
        TransferMode::Download,
        Some("bytes=-5"),
    )
    .await
    .unwrap();
    assert_eq!(slice.body_len, 5);
    assert_eq!(read_body(slice).await.unwrap(), &content[95..]);

    // 越界 → RANGE_INVALID,绝不整文件回退
    let err = open_slice(
        verified(tmp.path(), "data.txt"),
        TransferMode::Download,
        Some("bytes=100-"),
    )
    .await
    .unwrap_err();
    assert_eq!(err.stable_code(), "TRANSFER_RANGE_INVALID");
}

#[tokio::test]
async fn preview_mode_enforces_type_limits() {
    let tmp = tempfile::tempdir().unwrap();
    // 文本类型 → inline;attachment 类型在 Preview 模式 → 类型原因
    write(
        &tmp.path().join("clip.mp4"),
        b"\x00\x00\x00\x18ftypmp42junkdata",
    );

    let err = open_slice(
        verified(tmp.path(), "clip.mp4"),
        TransferMode::Preview,
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(err.stable_code(), "FILE_TYPE_NOT_PREVIEWABLE");
    assert!(matches!(
        err,
        bridge::files::FilesError::NotPreviewable(NotPreviewableReason::TypeUnsupported)
    ));

    // 同一文件 Download 模式:允许(流式返回)
    let slice = open_slice(
        verified(tmp.path(), "clip.mp4"),
        TransferMode::Download,
        None,
    )
    .await
    .unwrap();
    assert_eq!(slice.content_type, "video/mp4");
    assert!(!slice.inline);
}
