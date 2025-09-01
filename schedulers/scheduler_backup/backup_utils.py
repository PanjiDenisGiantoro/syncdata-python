import os
import csv
import oracledb
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from db import get_oracle_connection_dbrbn, get_oracle_connection_billing
from logger_config import backup_logger as logger

class DatabaseBackup:
    def __init__(self, batch_size=1000000):
        self.batch_size = batch_size
        self.backup_dir = Path("backup") / datetime.now().strftime("%Y-%m-%d")
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def get_table_connection(self, table_name):
        """Determine which database connection to use based on table name"""
        if table_name in ['cms_cost_transit_v2', 'cms_cost_delivery_v2', 'mst_code', 'ora_user', 'mst_btbpbd']:
            return get_oracle_connection_dbrbn()
        elif table_name == 'ops_return_unpaid_2025':
            return get_oracle_connection_billing()
        else:
            return None

    def get_total_rows(self, cursor, table_name, where_clause):
        """Mendapatkan total baris dalam tabel dengan filter yang sesuai"""
        try:
            query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
            cursor.execute(query)
            count = cursor.fetchone()[0]
            logger.info(f"Jumlah baris di {table_name}: {count} {f'dengan filter: {where_clause}' if where_clause else ''}")
            return count
        except Exception as e:
            logger.error(f"Gagal menghitung total baris untuk {table_name}: {str(e)}")
            return 0

    def get_where_clause(self, table_name):
        """Mendapatkan klausa WHERE berdasarkan nama tabel"""
        # Format tanggal kemarin dalam format Oracle
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%d-%b-%Y').upper()
        
        # Sesuaikan filter berdasarkan nama tabel
        if table_name == 'cms_cost_transit_v2':
            return f"WHERE TRUNC(transit_manifest_date) = TO_DATE('{yesterday}', 'DD-MON-YYYY')"
        elif table_name == 'cms_cost_delivery_v2':
            return f"WHERE TRUNC(cnote_crdate) = TO_DATE('{yesterday}', 'DD-MON-YYYY')"
        elif table_name == 'ops_return_unpaid_2025':
            return f"WHERE TRUNC(ops_rup_ins_date) = TO_DATE('{yesterday}', 'DD-MON-YYYY')"
        
        # Untuk tabel lain, tidak ada filter
        return ""

    def backup_table_to_csv(self, table_name):
        """Backup tabel ke CSV dengan pemrosesan batch dan part file"""
        try:
            conn = self.get_table_connection(table_name)
            if not conn:
                logger.error(f"Tidak ada koneksi untuk tabel: {table_name}")
                return

            with conn.cursor() as cursor:
                # Dapatkan klausa WHERE yang sesuai
                where_clause = self.get_where_clause(table_name)
                
                # Dapatkan total baris untuk pelacakan kemajuan
                total_rows = self.get_total_rows(cursor, table_name, where_clause)
                
                if total_rows == 0:
                    logger.warning(f"Tabel {table_name} kosong, melewati backup")
                    return

                # Get column names
                cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM = 0")
                columns = [desc[0] for desc in cursor.description]
                
                # Hitung jumlah part yang dibutuhkan
                part_size = 1000000  # 1 juta baris per part
                total_parts = (total_rows + part_size - 1) // part_size
                
                logger.info(f"Memulai backup {table_name} dengan {total_rows} baris ({total_parts} part)")
                
                for part in range(1, total_parts + 1):
                    # Siapkan nama file dengan nomor part
                    output_file = self.backup_dir / f"{table_name}_part{part:03d}.csv"
                    start_row = (part - 1) * part_size
                    end_row = part * part_size
                    
                    logger.info(f"Memproses part {part}/{total_parts} ({start_row+1}-{min(end_row, total_rows)} baris)")
                    
                    with open(output_file, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(columns)
                        
                        # Proses dalam batch
                        for offset in range(start_row, end_row, self.batch_size):
                            # Gunakan where_clause yang sudah dibuat sebelumnya
                            base_query = f"SELECT * FROM {table_name} {where_clause}"
                            query = f"""
                            SELECT * FROM (
                                SELECT a.*, ROW_NUMBER() OVER (ORDER BY ROWID) as rnum 
                                FROM ({base_query}) a 
                            ) 
                            WHERE rnum > :start_row AND rnum <= :end_row
                            """
                            
                            cursor.execute(query, {
                                'start_row': offset,
                                'end_row': min(offset + self.batch_size, end_row)
                            })
                            
                            batch = cursor.fetchall()
                            if not batch:
                                break
                                
                            writer.writerows(batch)
                            
                            # Log kemajuan
                            processed = min(offset + len(batch), end_row, total_rows)
                            logger.info(f"  Part {part}: {processed-start_row}/{end_row-start_row} baris ({processed}/{total_rows} total)")
                    
                    logger.info(f"Berhasil menyimpan part {part} ke {output_file}")
                
                logger.info(f"Selesai backup {table_name} dalam {total_parts} part")
                
        except Exception as e:
            logger.error(f"Error backing up table {table_name}: {str(e)}", exc_info=True)
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    def backup_all_tables(self, table_names):
        """Backup multiple tables in parallel"""
        with ThreadPoolExecutor() as executor:
            list(tqdm(
                executor.map(self.backup_table_to_csv, table_names),
                total=len(table_names),
                desc="Backing up tables"
            ))


def run_backup():
    """Run backup for all specified tables"""
    tables_to_backup = [
        'cms_cost_transit_v2',
        'cms_cost_delivery_v2',
        'mst_code',
        'ora_user',
        'mst_btbpbd',
        'ops_return_unpaid_2025'
    ]
    
    logger.info("Starting database backup process")
    backup = DatabaseBackup()
    backup.backup_all_tables(tables_to_backup)
    logger.info("Database backup process completed")
