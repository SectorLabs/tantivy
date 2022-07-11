use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use tantivy::directory::{CacheDirectory, MmapDirectory};
use tantivy::schema::{TEXT, Schema};
use tantivy::{doc, Index, IndexBuilder};

pub fn nrt_indexing_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("nrt-indexing");
    group.sample_size(10);
    let mut schema_builder = Schema::builder();
    let text_field = schema_builder.add_text_field("text", TEXT);
    let schema = schema_builder.build();
    group.bench_function("nrt-indexing", |b| {
        b.iter(|| {
            // let index = Index::create_from_tempdir(schema.clone()).unwrap();
            let dir = CacheDirectory::create(MmapDirectory::create_from_tempdir().unwrap());
            let index = IndexBuilder::new().schema(schema.clone()).open_or_create(dir).unwrap();
            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
                for _ in 0..4 {
                    index_writer.add_document(doc!(text_field=>"qwerty")).unwrap();
                    index_writer.soft_commit().unwrap();
                }
            }
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = nrt_indexing_benchmark
}
criterion_main!(benches);
