use std::path::Path;

use geo_types::Geometry;
use geozero::wkb::FromWkb;

use geo::algorithm::BoundingRect;
use geozero::wkb::WkbDialect;

fn main() {
    let f = std::fs::File::open(&Path::new(DATA_FILE)).unwrap();
    let data: Data = bson::from_reader(f).unwrap();

    let count = 200;
    let geoms = data
        .data
        .iter()
        .map(|x| geometry_from_ewkb(x.as_slice()))
        .collect::<Vec<_>>();
    let now = std::time::Instant::now();
    for _ in 0..count {
        for g in data.data.iter() {
            let g = geometry_from_ewkb(g.as_slice());
            let _b = g.bounding_rect();
        }
    }
    println!(
        "parse geometry and bounding_rect {:?}s total run:{}",
        now.elapsed().as_secs(),
        data.data.len() * count
    );
    let now = std::time::Instant::now();
    for _ in 0..count {
        for g in geoms.iter() {
            let _b = g.bounding_rect();
        }
    }
    println!(
        "bounding_rect {:?}s total run:{}",
        now.elapsed().as_secs(),
        data.data.len() * count
    );
}

fn geometry_from_ewkb(x: &[u8]) -> Geometry {
    let dialect = WkbDialect::Ewkb;
    let mut rdr = std::io::Cursor::new(x);
    let value = Geometry::from_wkb(&mut rdr, dialect).unwrap();
    value
}

const DATA_FILE: &str = "./data.bson";

#[derive(serde::Serialize, serde::Deserialize)]
struct Data {
    data: Vec<Vec<u8>>,
}

#[cfg(test)]
mod test {
    use std::ops::DerefMut;
    use std::path::Path;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::Row;

    use crate::{Data, DATA_FILE};
    #[tokio::test]
    async fn pull_data() {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect("postgres://postgres:zalando@192.168.0.215:2895/data-storage")
            .await
            .unwrap();
        let sql = format!("select geom from \"01799d3c388d2c36c4492b8ebc1dd179\"");
        let mut conn = pool.acquire().await.unwrap();

        let rows = sqlx::query(&sql).fetch_all(conn.deref_mut()).await.unwrap();

        let mut data = Vec::with_capacity(rows.len());

        for r in rows.iter() {
            let r = r.try_get_raw(0).unwrap();
            let value = r.as_bytes().ok().map(|x| x.to_vec());
            if value.is_some() {
                data.push(value.unwrap());
            }
        }

        let d = Data { data: data };

        let vec = bson::to_vec(&d).unwrap();

        let mut opt = std::fs::OpenOptions::new();
        opt.write(true);
        opt.create(true);
        opt.truncate(true);

        let mut f = opt.open(&Path::new(DATA_FILE)).unwrap();
        use std::io::Write;
        f.write_all(&vec).unwrap();
    }
}
