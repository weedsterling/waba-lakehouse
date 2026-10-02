-- Réinitialise les tables de faits Silver (changement de partitionnement journalier -> mensuel).
-- Sans risque : la couche Silver est entièrement recalculable depuis Bronze (principe médaillon).
DROP TABLE IF EXISTS lakehouse.silver.bank_transactions;
DROP TABLE IF EXISTS lakehouse.silver.insurance_operations;
DROP TABLE IF EXISTS lakehouse.silver.mobile_money_payments;
DROP TABLE IF EXISTS lakehouse.silver.loan_repayments;
