use std::io::Write;
use std::process::Command;

use assert_cmd::prelude::*;
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;
use tempfile::NamedTempFile;

#[test]
fn search() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("searcher")?;

    let mut queries = NamedTempFile::new()?;
    writeln!(queries, "tests/data/genome-s10.fa.gz.sig")?;

    let mut catalog = NamedTempFile::new()?;
    writeln!(catalog, "tests/data/genome-s10.fa.gz.sig")?;
    writeln!(catalog, "tests/data/genome-s11.fa.gz.sig")?;
    writeln!(catalog, "tests/data/genome-s12.fa.gz.sig")?;

    cmd.args(&["--threshold", "0"])
        .args(&["-k", "31"])
        .args(&["--scaled", "10000"])
        .arg(queries.path())
        .arg(catalog.path())
        .assert()
        .success()
        .stdout(contains("query,Run,containment"))
        .stdout(contains(
            "../genome-s10.fa.gz','tests/data/genome-s10.fa.gz.sig',1",
        ));

    Ok(())
}

#[test]
fn search_downsample() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("searcher")?;

    let mut queries = NamedTempFile::new()?;
    writeln!(queries, "tests/data/genome-s10.fa.gz.sig")?;

    let mut catalog = NamedTempFile::new()?;
    writeln!(catalog, "tests/data/genome-s10.fa.gz.sig")?;
    writeln!(catalog, "tests/data/genome-s11.fa.gz.sig")?;
    writeln!(catalog, "tests/data/genome-s12.fa.gz.sig")?;

    cmd.args(&["--threshold", "0"])
        .args(&["-k", "31"])
        .args(&["--scaled", "20000"])
        .arg(queries.path())
        .arg(catalog.path())
        .assert()
        .success()
        .stdout(contains("query,Run,containment"))
        .stdout(contains(
            "../genome-s10.fa.gz','tests/data/genome-s10.fa.gz.sig',1",
        ));

    Ok(())
}

#[test]
fn search_empty_query() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("searcher")?;

    let queries = NamedTempFile::new()?;

    let mut catalog = NamedTempFile::new()?;
    writeln!(catalog, "tests/data/genome-s10.fa.gz.sig")?;
    writeln!(catalog, "tests/data/genome-s11.fa.gz.sig")?;
    writeln!(catalog, "tests/data/genome-s12.fa.gz.sig")?;

    cmd.args(&["--threshold", "0"])
        .args(&["-k", "31"])
        .args(&["--scaled", "10000"])
        .arg(queries.path())
        .arg(catalog.path())
        .assert()
        .success()
        .stderr(contains("No query signatures loaded, exiting."))
        .stdout(contains("query,Run,containment").not());

    Ok(())
}

#[test]
fn search_catalog_empty_line() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("searcher")?;

    let mut queries = NamedTempFile::new()?;
    writeln!(queries, "tests/data/genome-s10.fa.gz.sig")?;

    let mut catalog = NamedTempFile::new()?;
    writeln!(catalog, "tests/data/genome-s10.fa.gz.sig")?;
    writeln!(catalog, "")?;

    cmd.args(&["--threshold", "0"])
        .args(&["-k", "31"])
        .args(&["--scaled", "10000"])
        .arg(queries.path())
        .arg(catalog.path())
        .assert()
        .success()
        .stdout(contains("query,Run,containment"))
        .stdout(contains(
            "../genome-s10.fa.gz','tests/data/genome-s10.fa.gz.sig',1",
        ));

    Ok(())
}

#[test]
fn search_queries_empty_line() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("searcher")?;

    let mut queries = NamedTempFile::new()?;
    writeln!(queries, "tests/data/genome-s10.fa.gz.sig")?;
    writeln!(queries, "")?;

    let mut catalog = NamedTempFile::new()?;
    writeln!(catalog, "tests/data/genome-s10.fa.gz.sig")?;

    cmd.args(&["--threshold", "0"])
        .args(&["-k", "31"])
        .args(&["--scaled", "10000"])
        .arg(queries.path())
        .arg(catalog.path())
        .assert()
        .success()
        .stdout(contains("query,Run,containment"))
        .stdout(contains(
            "../genome-s10.fa.gz','tests/data/genome-s10.fa.gz.sig',1",
        ));

    Ok(())
}
