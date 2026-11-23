use super::*;

#[test]
fn parse_simple_count() {
    let q = parse_sql_v0("SELECT COUNT(*) FROM hits;").unwrap();
    assert_eq!(q.from.0, "hits");
    assert_eq!(q.select.len(), 1);
}

#[test]
fn parse_create_model_uses_select_parser_and_defaults() {
    let statement = parse_statement(
        "CREATE MODEL customer_segments OPTIONS (model_type = 'kmeans', num_clusters = 12) AS SELECT income, spending FROM customers WHERE income > 0;",
    ).unwrap();
    let Statement::CreateModel {
        name,
        options,
        query,
        query_sql,
    } = statement
    else {
        panic!("expected model statement")
    };
    assert_eq!(name, "customer_segments");
    assert_eq!(options.clusters, 12);
    assert_eq!(options.max_iterations, 100);
    assert_eq!(options.seed, 42);
    assert_eq!(options.tolerance, 1e-4);
    assert_eq!(query.from.0, "customers");
    assert!(query_sql.starts_with("SELECT"));
    assert!(query.where_.is_some());
    assert_eq!(query.select.len(), 2);
}

#[test]
fn create_model_rejects_unknown_duplicate_and_invalid_options() {
    for sql in [
        "CREATE MODEL m OPTIONS (model_type='kmeans', mystery=2) AS SELECT x FROM t",
        "CREATE MODEL m OPTIONS (model_type='kmeans', seed=1, seed=2) AS SELECT x FROM t",
        "CREATE MODEL m OPTIONS (model_type='regression') AS SELECT x FROM t",
        "CREATE MODEL m OPTIONS (model_type='kmeans', num_clusters=0) AS SELECT x FROM t",
    ] {
        assert!(parse_statement(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parse_where_group_order_limit() {
    let sql = "SELECT AdvEngineID, COUNT(*) AS c \
               FROM hits WHERE AdvEngineID <> 0 \
               GROUP BY AdvEngineID \
               ORDER BY c DESC LIMIT 10;";
    let q = parse_sql_v0(sql).unwrap();
    assert!(q.where_.is_some());
    assert_eq!(q.group_by.len(), 1);
    assert_eq!(q.order_by.len(), 1);
    assert_eq!(q.limit, Some(10));
}

#[test]
fn parse_like() {
    let sql = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';";
    let q = parse_sql_v0(sql).unwrap();
    assert!(q.where_.is_some());
}

#[test]
fn parse_count_distinct() {
    let sql = "SELECT COUNT(DISTINCT UserID) FROM hits;";
    let q = parse_sql_v0(sql).unwrap();
    assert_eq!(q.select.len(), 1);
    assert_eq!(q.from.0, "hits");
}

#[test]
fn limit_and_offset_in_either_order() {
    for sql in [
        "SELECT COUNT(*) FROM hits LIMIT 10 OFFSET 100",
        "SELECT COUNT(*) FROM hits OFFSET 100 LIMIT 10",
    ] {
        let query = parse_sql_v0(sql).unwrap();
        assert_eq!((query.limit, query.offset), (Some(10), Some(100)), "{sql}");
    }
    assert!(parse_sql_v0("SELECT COUNT(*) FROM hits LIMIT 1 LIMIT 2").is_err());
}

#[test]
fn parse_approx_count_distinct() {
    let sql = "SELECT approx_count_distinct(UserID) FROM hits;";
    let q = parse_sql_v0(sql).unwrap();
    assert_eq!(q.select.len(), 1);
    assert_eq!(q.from.0, "hits");
    match &q.select[0].expr {
        Expr::Agg { func, arg, .. } => {
            assert_eq!(*func, AggFn::ApproxDistinct);
            assert!(matches!(arg.as_ref(), Expr::Ident(_)));
        }
        _ => panic!("expected Agg"),
    }
}

#[test]
fn parse_approx_count_distinct_star_fails() {
    let sql = "SELECT approx_count_distinct(*) FROM hits;";
    let result = parse_sql_v0(sql);
    assert!(result.is_err(), "approx_count_distinct(*) should fail");
}

#[test]
fn parse_multiple_aggs_and_alias() {
    let sql = "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;";
    let q = parse_sql_v0(sql).unwrap();
    assert_eq!(q.select.len(), 2);
    assert_eq!(q.group_by.len(), 1);
    assert_eq!(q.order_by.len(), 1);
    assert_eq!(q.limit, Some(10));
}

#[test]
fn parse_offset_limit() {
    // Parser expects OFFSET then LIMIT (v0)
    let sql = "SELECT URL FROM hits ORDER BY EventTime OFFSET 1000 LIMIT 10;";
    let q = parse_sql_v0(sql).unwrap();
    assert_eq!(q.offset, Some(1000));
    assert_eq!(q.limit, Some(10));
}

#[test]
fn parse_having() {
    let sql = "SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY c DESC LIMIT 25;";
    let q = parse_sql_v0(sql).unwrap();
    assert!(q.having.is_some());
    assert_eq!(q.limit, Some(25));
}

#[test]
fn parse_no_semicolon() {
    let q = parse_sql_v0("SELECT 1 FROM t").unwrap();
    assert_eq!(q.from.0, "t");
}

#[test]
fn parse_star_select() {
    let q = parse_sql_v0("SELECT * FROM hits LIMIT 10").unwrap();
    assert_eq!(q.select.len(), 1);
    assert!(matches!(q.select[0].expr, Expr::Star));
}

#[test]
fn parse_function_call() {
    let q = parse_sql_v0("SELECT length(URL) FROM hits").unwrap();
    assert_eq!(q.select.len(), 1);
    match &q.select[0].expr {
        Expr::Call { name, args } => {
            assert_eq!(name.0, "length");
            assert_eq!(args.len(), 1);
            assert!(matches!(args[0], Expr::Ident(_)));
        }
        _ => panic!("expected Call"),
    }
}

#[test]
fn parse_agg_of_call() {
    let q = parse_sql_v0("SELECT AVG(length(URL)) AS l FROM hits").unwrap();
    assert_eq!(q.select.len(), 1);
    match &q.select[0].expr {
        Expr::Agg { arg, .. } => match arg.as_ref() {
            Expr::Call { name, args } => {
                assert_eq!(name.0, "length");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("expected Call inside Agg"),
        },
        _ => panic!("expected Agg"),
    }
}

#[test]
fn parse_in() {
    let q = parse_sql_v0("SELECT 1 FROM hits WHERE TraficSourceID IN (-1, 6)").unwrap();
    assert!(q.where_.is_some());
    match q.where_.as_ref().unwrap() {
        Expr::In {
            left,
            values,
            negated,
        } => {
            assert!(!negated);
            assert!(matches!(left.as_ref(), Expr::Ident(_)));
            assert_eq!(values.len(), 2);
        }
        _ => panic!("expected In"),
    }
}

#[test]
fn parse_not_in() {
    let q = parse_sql_v0("SELECT 1 FROM hits WHERE col NOT IN (1, 2, 3)").unwrap();
    match q.where_.as_ref().unwrap() {
        Expr::In {
            negated, values, ..
        } => {
            assert!(*negated);
            assert_eq!(values.len(), 3);
        }
        _ => panic!("expected In"),
    }
}

/// Rewrites that avoid parser-unsupported syntax (see readme).
#[test]
fn parse_rewritten_extract_and_date() {
    // #19 rewrite: extract('minute', EventTime) instead of extract(minute FROM EventTime)
    let q = parse_sql_v0("SELECT UserID, extract('minute', EventTime) AS m FROM hits GROUP BY UserID, extract('minute', EventTime)").unwrap();
    assert_eq!(q.select.len(), 2);
    assert_eq!(q.group_by.len(), 2);
    // #37-style rewrite: DATE '...' → '...'
    let q2 = parse_sql_v0("SELECT URL, COUNT(*) AS c FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' GROUP BY URL").unwrap();
    assert!(q2.where_.is_some());
    assert_eq!(q2.group_by.len(), 1);
    // #41: IN is supported; with DATE→string rewrite the full query parses
    let q3 = parse_sql_v0("SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC OFFSET 100 LIMIT 10").unwrap();
    assert!(q3.where_.is_some());
    assert_eq!(q3.offset, Some(100));
    assert_eq!(q3.limit, Some(10));
}

/// All SELECT queries from readme.md (V1 target); v0 parser supports a subset.
const README_QUERIES: &[&str] = &[
    "SELECT COUNT(*) FROM hits;",
    "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;",
    "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;",
    "SELECT AVG(UserID) FROM hits;",
    "SELECT COUNT(DISTINCT UserID) FROM hits;",
    "SELECT COUNT(DISTINCT SearchPhrase) FROM hits;",
    "SELECT MIN(EventDate), MAX(EventDate) FROM hits;",
    "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;",
    "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;",
    "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;",
    "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;",
    "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;",
    "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
    "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;",
    "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;",
    "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;",
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;",
    "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;",
    "SELECT UserID, extract('minute', EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract('minute', EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;",
    "SELECT UserID FROM hits WHERE UserID = 435090932899640449;",
    "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';",
    "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
    "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
    "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;",
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;",
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;",
    "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;",
    "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;",
    "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;",
    "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;",
    "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;",
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;",
    "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;",
    "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;",
    "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;",
    "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;",
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;",
    "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;",
    "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;",
    "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END, URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;",
    "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC OFFSET 100 LIMIT 10;",
    "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC OFFSET 10000 LIMIT 10;",
    "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) OFFSET 1000 LIMIT 10;",
];

#[test]
fn readme_queries_parse_count() {
    let mut ok = 0;
    let mut fail = Vec::new();
    for (i, sql) in README_QUERIES.iter().enumerate() {
        match parse_sql_v0(sql) {
            Ok(_) => ok += 1,
            Err(e) => fail.push((i + 1, sql, e)),
        }
    }
    eprintln!(
        "readme: {} parsed, {} failed (of {})",
        ok,
        fail.len(),
        README_QUERIES.len()
    );
    for (idx, sql, e) in &fail {
        eprintln!(
            "  #{} at {}: {} ... {:?}",
            idx,
            e.at,
            e.msg,
            &sql[..sql.len().min(80)]
        );
    }
    assert_eq!(
        ok,
        42,
        "readme: 42 of 43 queries parse (only #40 CASE unparsed); {} parsed, {} failed",
        ok,
        fail.len()
    );
}
