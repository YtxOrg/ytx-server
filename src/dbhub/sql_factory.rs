use crate::constant::*;
use crate::dbhub::SqlGen;
use crate::dbhub::section::*;

use std::collections::HashMap;
pub struct SqlFactory {
    gens: HashMap<&'static str, Box<dyn SqlGen>>,
}

impl SqlFactory {
    pub fn new() -> Self {
        let mut gens: HashMap<&'static str, Box<dyn SqlGen>> = HashMap::new();

        gens.insert(FINANCE, Box::new(Finance {}));
        gens.insert(STAKEHOLDER, Box::new(Stakeholder {}));
        gens.insert(ITEM, Box::new(Item {}));
        gens.insert(TASK, Box::new(Task {}));
        gens.insert(SALE, Box::new(Sale {}));
        gens.insert(PURCHASE, Box::new(Purchase {}));

        Self { gens }
    }

    pub fn get(&self, section: &str) -> Option<&dyn SqlGen> {
        self.gens.get(section).map(|b| b.as_ref())
    }
}
