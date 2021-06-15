use futures::executor::block_on;
use anyhow::Result;
use blocksy3 as db;

fn run(script: &str) -> Result<()> {
    let tokens = script.split(&[' ', '\n'][..]).map(String::from);
    let (path, commands) = db::cmdscript::parse_commands(tokens)?;
    block_on(db::cmdscript::exec(path, commands))?;
    Ok(())
}

#[test]
#[should_panic]
fn read_assert_some_panic_1() {
    run("

mem
write t1 k1 v1
read-assert t1 k1 v0

").unwrap()
}

#[test]
#[should_panic]
fn read_assert_some_panic_2() {
    run("

mem
read-assert t1 k1 v0

").unwrap()
}

#[test]
#[should_panic]
fn read_assert_none_panic() {
    run("

mem
write t1 k1 v1
read-assert t1 k1 <none>

").unwrap()
}

#[test]
fn delete_range_same_batch_as_write_1() -> Result<()> {
    run("

mem
batch-open b1
batch-write b1 t1 k1 v1
batch-delete-range b1 t1 k1 k2
batch-commit b1
batch-close b1
read-assert t1 k1 <none>

")
}

#[test]
fn delete_range_same_batch_as_write_2() -> Result<()> {
    run("

mem
batch-open b1
batch-write b1 t1 k1 v1
batch-delete-range b1 t1 k1 k2
batch-write b1 t1 k1 v1
batch-commit b1
batch-close b1
read-assert t1 k1 v1

")
}
