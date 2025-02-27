use csv::{ReaderBuilder, WriterBuilder};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::File;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
struct TransactionRecord {
    #[serde(rename = "type")]
    tx_type: TransactionType,
    client: u16,
    tx: u32,
    // For deposits and withdrawals this should be Some(amount). For dispute/resolve/chargeback it is None.
    amount: Option<Decimal>,
}

#[derive(Debug)]
struct DepositTx {
    amount: Decimal,
    disputed: bool,
}

#[derive(Debug)]
struct Account {
    available: Decimal,
    held: Decimal,
    locked: bool,
    // Deposit history per account, keyed by transaction id.
    deposits: HashMap<u32, DepositTx>,
}

impl Account {
    fn new() -> Self {
        Account {
            available: Decimal::new(0, 4),
            held: Decimal::new(0, 4),
            locked: false,
            deposits: HashMap::new(),
        }
    }
}

#[derive(Serialize)]
struct OutputRecord {
    client: u16,
    available: String,
    held: String,
    total: String,
    locked: bool,
}

/// Processes an iterator of transactions and returns the resulting accounts state.
fn process_records<I: Iterator<Item = TransactionRecord>>(records: I) -> HashMap<u16, Account> {
    let mut accounts: HashMap<u16, Account> = HashMap::new();

    for record in records {
        // Ensure the account exists.
        let account = accounts.entry(record.client).or_insert_with(Account::new);

        // Ignore any transaction if the account is locked.
        if account.locked {
            continue;
        }

        match record.tx_type {
            TransactionType::Deposit => {
                if let Some(amount) = record.amount {
                    account.available += amount;
                    account.deposits.insert(
                        record.tx,
                        DepositTx {
                            amount,
                            disputed: false,
                        },
                    );
                }
            }
            TransactionType::Withdrawal => {
                if let Some(amount) = record.amount {
                    if account.available >= amount {
                        account.available -= amount;
                    }
                }
            }
            TransactionType::Dispute => {
                if let Some(deposit) = account.deposits.get_mut(&record.tx) {
                    // Only process dispute if not already disputed.
                    if !deposit.disputed {
                        account.available -= deposit.amount;
                        account.held += deposit.amount;
                        deposit.disputed = true;
                    }
                }
            }
            TransactionType::Resolve => {
                if let Some(deposit) = account.deposits.get_mut(&record.tx) {
                    if deposit.disputed {
                        account.held -= deposit.amount;
                        account.available += deposit.amount;
                        deposit.disputed = false;
                    }
                }
            }
            TransactionType::Chargeback => {
                if let Some(deposit) = account.deposits.get_mut(&record.tx) {
                    if deposit.disputed {
                        account.held -= deposit.amount;
                        account.locked = true;
                        deposit.disputed = false;
                    }
                }
            }
        }
    }
    accounts
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <transactions.csv>", args[0]);
        std::process::exit(1);
    }
    let input_path = &args[1];

    let file = File::open(input_path)?;
    let mut rdr = ReaderBuilder::new().trim(csv::Trim::All).from_reader(file);

    // Process transactions from the CSV, ignoring any deserialization errors.
    let accounts = process_records(rdr.deserialize().filter_map(Result::ok));

    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_writer(std::io::stdout());

    for (&client, account) in &accounts {
        let total = account.available + account.held;
        let output = OutputRecord {
            client,
            available: format!("{:.4}", account.available),
            held: format!("{:.4}", account.held),
            total: format!("{:.4}", total),
            locked: account.locked,
        };
        wtr.serialize(output)?;
    }
    wtr.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_deposit() {
        let client_id = 1u16;
        // Create a deposit transaction for client 1.
        let records = vec![TransactionRecord {
            tx_type: TransactionType::Deposit,
            client: client_id,
            tx: 1,
            amount: Some(Decimal::from_str("1.0").unwrap()),
        }];
        let accounts = process_records(records.into_iter());
        let account = accounts.get(&client_id).unwrap();
        // After deposit, available funds should equal the deposit amount.
        assert_eq!(account.available, Decimal::from_str("1.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);
    }
    #[test]
    fn test_deposit_multi_account() {
        let client_id = 1u16;
        let client_id_2 = 2u16;

        // Create a deposit transaction for client 1.
        let records = vec![
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: client_id,
                tx: 1,
                amount: Some(Decimal::from_str("1.0").unwrap()),
            },
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: client_id_2,
                tx: 1,
                amount: Some(Decimal::from_str("2.0").unwrap()),
            },
        ];
        let accounts = process_records(records.into_iter());

        let account = accounts.get(&client_id).unwrap();
        // After deposit, available funds should equal the deposit amount.
        assert_eq!(account.available, Decimal::from_str("1.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);

        let account = accounts.get(&client_id_2).unwrap();
        // After deposit, available funds should equal the deposit amount.
        assert_eq!(account.available, Decimal::from_str("2.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);
    }

    #[test]
    fn test_withdrawal_success() {
        // Deposit 2.0 and then withdraw 1.5 from client 1.
        let client_id = 1u16;
        let records = vec![
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: client_id,
                tx: 1,
                amount: Some(Decimal::from_str("2.0").unwrap()),
            },
            TransactionRecord {
                tx_type: TransactionType::Withdrawal,
                client: client_id,
                tx: 2,
                amount: Some(Decimal::from_str("1.5").unwrap()),
            },
        ];
        let accounts = process_records(records.into_iter());
        let account = accounts.get(&client_id).unwrap();
        // Withdrawal should deduct the funds, leaving 0.5 available.
        assert_eq!(account.available, Decimal::from_str("0.5").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);
    }

    #[test]
    fn test_withdrawal_multi_account() {
        // Deposit 2.0 and then withdraw 1.5 from client 1.
        let client_id = 1u16;
        let client_id_2 = 2u16;
        let records = vec![
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: client_id,
                tx: 1,
                amount: Some(Decimal::from_str("2.0").unwrap()),
            },
            TransactionRecord {
                tx_type: TransactionType::Withdrawal,
                client: client_id,
                tx: 2,
                amount: Some(Decimal::from_str("1.5").unwrap()),
            },
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: client_id_2,
                tx: 1,
                amount: Some(Decimal::from_str("3.0").unwrap()),
            },
            TransactionRecord {
                tx_type: TransactionType::Withdrawal,
                client: client_id_2,
                tx: 2,
                amount: Some(Decimal::from_str("1.5").unwrap()),
            },
        ];
        let accounts = process_records(records.into_iter());
        let account = accounts.get(&client_id).unwrap();
        // Withdrawal should deduct the funds, leaving 0.5 available.
        assert_eq!(account.available, Decimal::from_str("0.5").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);

        let account = accounts.get(&client_id_2).unwrap();
        assert_eq!(account.available, Decimal::from_str("1.5").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);
    }
    #[test]
    fn test_withdrawal_insufficient_funds() {
        // Deposit 1.0 and attempt to withdraw 1.5 from client 1.
        let client_id = 1u16;
        let records = vec![
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: client_id,
                tx: 1,
                amount: Some(Decimal::from_str("1.0").unwrap()),
            },
            TransactionRecord {
                tx_type: TransactionType::Withdrawal,
                client: client_id,
                tx: 2,
                amount: Some(Decimal::from_str("1.5").unwrap()),
            },
        ];
        let accounts = process_records(records.into_iter());
        let account = accounts.get(&client_id).unwrap();
        // Since funds are insufficient, the withdrawal should not occur.
        assert_eq!(account.available, Decimal::from_str("1.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);
    }
    #[test]
    fn test_dispute() {
        // This test verifies that a dispute against a deposit properly moves funds
        // from available to held. If a dispute is issued for a non-existent deposit,
        // it is ignored.
        let records = vec![
            // Client 1 deposits 2.0 with transaction id 10.
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: 1,
                tx: 10,
                amount: Some(Decimal::from_str("2.0").unwrap()),
            },
            // Client 1 disputes transaction id 10.
            TransactionRecord {
                tx_type: TransactionType::Dispute,
                client: 1,
                tx: 10,
                amount: None,
            },
            // Client 1 attempts to dispute a non-existent transaction id 99 (should be ignored).
            TransactionRecord {
                tx_type: TransactionType::Dispute,
                client: 1,
                tx: 99,
                amount: None,
            },
        ];

        let accounts = process_records(records.into_iter());
        let account = accounts.get(&1).unwrap();

        // After the dispute on tx 10:
        // - Available funds should decrease by 2.0.
        // - Held funds should increase by 2.0.
        // - The non-existent dispute has no effect.
        assert_eq!(account.available, Decimal::from_str("0.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("2.0").unwrap());
        assert!(!account.locked);
    }

    #[test]
    fn test_resolve() {
        // This test verifies that a dispute against a deposit properly moves funds
        // from available to held. If a dispute is issued for a non-existent deposit,
        // it is ignored.
        let records = vec![
            // Client 1 deposits 2.0 with transaction id 10.
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: 1,
                tx: 10,
                amount: Some(Decimal::from_str("2.0").unwrap()),
            },
            // Client 1 disputes transaction id 10.
            TransactionRecord {
                tx_type: TransactionType::Dispute,
                client: 1,
                tx: 10,
                amount: None,
            },
            // Client 1 attempts to dispute a non-existent transaction id 99 (should be ignored).
            TransactionRecord {
                tx_type: TransactionType::Resolve,
                client: 1,
                tx: 10,
                amount: None,
            },
        ];

        let accounts = process_records(records.into_iter());
        let account = accounts.get(&1).unwrap();

        assert_eq!(account.available, Decimal::from_str("2.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(!account.locked);
    }

    #[test]
    fn test_chargeback() {
        // Client 1 deposits 2.0, disputes it, and then chargebacks the disputed deposit.
        let records = vec![
            // Deposit: increases available funds.
            TransactionRecord {
                tx_type: TransactionType::Deposit,
                client: 1,
                tx: 10,
                amount: Some(Decimal::from_str("2.0").unwrap()),
            },
            // Dispute: moves funds from available to held.
            TransactionRecord {
                tx_type: TransactionType::Dispute,
                client: 1,
                tx: 10,
                amount: None,
            },
            // Chargeback: withdraws the held funds and freezes the account.
            TransactionRecord {
                tx_type: TransactionType::Chargeback,
                client: 1,
                tx: 10,
                amount: None,
            },
        ];

        let accounts = process_records(records.into_iter());
        let account = accounts.get(&1).unwrap();

        // After the dispute, available would have dropped by 2.0 and held increased by 2.0.
        // Then, after the chargeback, the held funds are removed (withdrawn) so held becomes 0,
        // and since available remains unchanged, total funds (available + held) become 0.
        // Additionally, the account is frozen.
        assert_eq!(account.available, Decimal::from_str("0.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("0.0").unwrap());
        assert!(account.locked);
    }
}
