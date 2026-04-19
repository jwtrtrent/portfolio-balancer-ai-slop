use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::id::{AccountId, SecurityId, SleeveId};

/// Read-only registry of names <-> ids. Implementors must be cheap to clone
/// or shareable behind an `Arc` and safe to query from any thread.
pub trait Registry: Send + Sync {
    fn account_id(&self, name: &str) -> Option<AccountId>;
    fn security_id(&self, name: &str) -> Option<SecurityId>;
    fn sleeve_id(&self, name: &str) -> Option<SleeveId>;

    fn account_name(&self, id: AccountId) -> Option<Arc<str>>;
    fn security_name(&self, id: SecurityId) -> Option<Arc<str>>;
    fn sleeve_name(&self, id: SleeveId) -> Option<Arc<str>>;
}

#[derive(Default)]
struct Interner {
    by_name: HashMap<Arc<str>, u32>,
    by_id: Vec<Arc<str>>,
}

impl Interner {
    fn intern(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.by_name.get(name) {
            return id;
        }
        let id = self.by_id.len() as u32;
        let arc: Arc<str> = Arc::from(name);
        self.by_id.push(Arc::clone(&arc));
        self.by_name.insert(arc, id);
        id
    }

    fn lookup(&self, name: &str) -> Option<u32> {
        self.by_name.get(name).copied()
    }

    fn reverse(&self, id: u32) -> Option<Arc<str>> {
        self.by_id.get(id as usize).cloned()
    }
}

/// Thread-safe interner for account/security/sleeve names. Each kind has an
/// independent id space so ids from different kinds never collide.
#[derive(Default)]
pub struct SharedRegistry {
    accounts: Mutex<Interner>,
    securities: Mutex<Interner>,
    sleeves: Mutex<Interner>,
}

impl SharedRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn intern_account(&self, name: &str) -> AccountId {
        AccountId(self.accounts.lock().expect("poisoned").intern(name))
    }

    pub fn intern_security(&self, name: &str) -> SecurityId {
        SecurityId(self.securities.lock().expect("poisoned").intern(name))
    }

    pub fn intern_sleeve(&self, name: &str) -> SleeveId {
        SleeveId(self.sleeves.lock().expect("poisoned").intern(name))
    }
}

impl Registry for SharedRegistry {
    fn account_id(&self, name: &str) -> Option<AccountId> {
        self.accounts
            .lock()
            .expect("poisoned")
            .lookup(name)
            .map(AccountId)
    }

    fn security_id(&self, name: &str) -> Option<SecurityId> {
        self.securities
            .lock()
            .expect("poisoned")
            .lookup(name)
            .map(SecurityId)
    }

    fn sleeve_id(&self, name: &str) -> Option<SleeveId> {
        self.sleeves
            .lock()
            .expect("poisoned")
            .lookup(name)
            .map(SleeveId)
    }

    fn account_name(&self, id: AccountId) -> Option<Arc<str>> {
        self.accounts.lock().expect("poisoned").reverse(id.0)
    }

    fn security_name(&self, id: SecurityId) -> Option<Arc<str>> {
        self.securities.lock().expect("poisoned").reverse(id.0)
    }

    fn sleeve_name(&self, id: SleeveId) -> Option<Arc<str>> {
        self.sleeves.lock().expect("poisoned").reverse(id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn interns_the_same_name_to_the_same_id() {
        let r = SharedRegistry::new();
        let a = r.intern_account("roth");
        let b = r.intern_account("roth");
        assert_eq!(a, b);
    }

    #[test]
    fn different_namespaces_have_independent_ids() {
        let r = SharedRegistry::new();
        let a = r.intern_account("x");
        let s = r.intern_security("x");
        // Raw ids may coincide (both start at 0); the types differ which is
        // the whole point.
        assert_eq!(a.raw(), s.raw());
    }

    #[test]
    fn reverse_lookup_returns_the_original_name() {
        let r = SharedRegistry::new();
        let id = r.intern_security("VTI");
        assert_eq!(&*r.security_name(id).unwrap(), "VTI");
    }

    #[test]
    fn lookup_returns_none_for_unknown() {
        let r = SharedRegistry::new();
        assert!(r.account_id("nope").is_none());
        assert!(r.security_name(SecurityId(42)).is_none());
    }

    #[test]
    fn concurrent_interns_are_consistent() {
        let r = Arc::new(SharedRegistry::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let r = Arc::clone(&r);
            handles.push(thread::spawn(move || {
                let a = r.intern_security("VTI");
                let b = r.intern_security("BND");
                (a, b)
            }));
        }
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let (first_a, first_b) = results[0];
        for (a, b) in &results[1..] {
            assert_eq!(*a, first_a);
            assert_eq!(*b, first_b);
        }
    }
}
