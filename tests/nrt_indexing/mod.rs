use tantivy::collector::Count;
use tantivy::directory::{CacheDirectory,  MmapDirectory};
use tantivy::query::AllQuery;
use tantivy::schema::{TEXT, SchemaBuilder};
use tantivy::{doc, IndexBuilder};


#[test]
fn test_soft_commit() -> tantivy::Result<()> {
    let mut builder = SchemaBuilder::new();
    let text_field = builder.add_text_field("text", TEXT);
    let schema = builder.build();
    let mmap_dir = MmapDirectory::create_from_tempdir()?;
    {
        let dir = CacheDirectory::create(mmap_dir.clone());
        let index = IndexBuilder::new().schema(schema.clone()).open_or_create(dir)?;

        let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
        index_writer.add_document(doc!(text_field=>"apple")).unwrap();
        index_writer.soft_commit().unwrap();

        let reader = index.reader()?;
        let searcher = reader.searcher();

        assert_eq!(1, searcher.search(&AllQuery, &Count)?);
    }

    let index = IndexBuilder::new().schema(schema).open_or_create(mmap_dir)?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    assert_eq!(0, searcher.search(&AllQuery, &Count)?);

    Ok(())
}
