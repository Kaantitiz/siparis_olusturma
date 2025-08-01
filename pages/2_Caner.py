import streamlit as st
import pandas as pd
from io import BytesIO
import datetime
import numpy as np
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import xlsxwriter  # Daha hızlı Excel yazma için

# Sayfa ayarları
st.set_page_config(
    page_title="Excel Dönüştürme Aracı (Optimize)",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Başlık
st.title("⚡ Ultra Hızlı Excel Dönüştürücü")
st.caption("60.000+ satırlık dosyalar için optimize edilmiş versiyon")

# Önbellek fonksiyonları
@st.cache_data(max_entries=3, show_spinner="Dosya okunuyor...")
def load_data(uploaded_file):
    """Büyük dosyalar için optimize edilmiş okuma"""
    return pd.read_excel(
        uploaded_file,
        dtype={
            'URUNKODU': 'string',
            'ACIKLAMA': 'string',
            'URETİCİKODU': 'string',
            'ORJİNAL': 'string',
            'ESKİKOD': 'string'
        }
    )

@st.cache_data(show_spinner="Veri dönüştürülüyor...")
def transform_data(df):
    """Ultra optimize dönüşüm fonksiyonu"""
    try:
        # Sütun optimizasyonu
        depo_prefixes = ['02-', '04-', 'D01-', 'A01-', 'TD-E01-', 'E01-']
        depo_cols = [
            f"{prefix}{col_type}"
            for prefix in depo_prefixes
            for col_type in ['DEVIR', 'ALIS', 'STOK', 'SATIS']
        ]
        
        required_cols = [
            'URUNKODU', 'ACIKLAMA', 'URETİCİKODU', 'ORJİNAL', 'ESKİKOD',
            'TOPL.FAT.ADT', 'MÜŞT.SAY.', 'SATıŞ FIYATı', 'DÖVIZ CINSI (S)'
        ] + [f'CAT{i}' for i in range(1, 8)] + depo_cols
        
        # Mevcut sütunları filtrele
        available_cols = [col for col in required_cols if col in df.columns]
        df_filtered = df[available_cols].copy()
        
        # Tam sıralama - istediğiniz şekilde
        new_df = pd.DataFrame()
        
        # 1. URUNKODU (ilk)
        new_df['URUNKODU'] = df_filtered['URUNKODU'].fillna('')
        
        # 2. URUNKODU (ikinci)
        new_df['URUNKODU_2'] = df_filtered['URUNKODU'].fillna('')
        
        # 3. Düzenlenmiş Ürün Kodu
        new_df['Düzenlenmiş Ürün Kodu'] = df_filtered['URUNKODU'].fillna('').str.replace(r'^[^-]*-', "'", regex=True)
        
        # 4. ACIKLAMA
        new_df['ACIKLAMA'] = df_filtered['ACIKLAMA'].fillna('')
        
        # 5. URETİCİKODU
        new_df['URETİCİKODU'] = df_filtered['URETİCİKODU'].fillna('')
        
        # 6. ORJİNAL
        new_df['ORJİNAL'] = df_filtered['ORJİNAL'].fillna('')
        
        # 7. ESKİKOD
        new_df['ESKİKOD'] = df_filtered['ESKİKOD'].fillna('')
        
        # 8. Kategoriler (CAT1-CAT7)
        for i in range(1, 8):
            cat_col = f'CAT{i}'
            if cat_col in df_filtered.columns:
                new_df[f'CAT{i}'] = df_filtered[cat_col].fillna('')
        
        # 9. Depo verileri - DEVIR, ALIŞ, SATIS, STOK (sıralama: MASLAK, İMES, İKİTELLİ, BOLU, ANKARA)
        depo_mapping = {
            '02-': 'MASLAK',
            'D01-': 'İMES',
            'TD-E01-': 'İKİTELLİ',
            'E01-': 'İKİTELLİ',
            '04-': 'BOLU',
            'A01-': 'ANKARA'
        }
        
        for old_prefix, new_name in depo_mapping.items():
            for col_type, new_type in zip(['DEVIR', 'ALIS', 'SATIS', 'STOK'],
                                         ['DEVIR', 'ALIŞ', 'SATIS', 'STOK']):
                old_col = f"{old_prefix}{col_type}"
                if old_col in df_filtered.columns:
                    try:
                        # Güvenli replace işlemi
                        col_data = df_filtered[old_col].fillna(0)
                        # Sayısal değerleri kontrol et
                        if pd.api.types.is_numeric_dtype(col_data):
                            col_data = col_data.astype(float)
                            col_data = col_data.replace(0, '-')
                        else:
                            col_data = col_data.astype(str)
                        new_df[f"{new_name} {new_type}"] = col_data.astype('string')
                    except Exception:
                        # Hata durumunda boş sütun
                        new_df[f"{new_name} {new_type}"] = '-'
        
        # 10. Boş sütunlar (İmesten İkitelli Depoya silindi)
        empty_cols = [
            'Not', 'Maslak Depo Bakiye', 'Bolu Depo Bakiye', 'İmes Depo Bakiye',
            'Ankara Depo Bakiye', 'İkitelli Depo Bakiye', 'Kampanya Tipi',
            'Toplam İsk', 'Toplam Depo Bakiye', 'Maslak Tedarikçi Bakiye',
            'Bolu Tedarikçi Bakiye', 'İmes Tedarikçi Bakiye',
            'Ankara Tedarikçi Bakiye', 'İkitelli Tedarikçi Bakiye',
            'Paket Adetleri', 'Maslak Sipariş',
            'Bolu Sipariş', 'İmes Sipariş', 'Ankara Sipariş', 'İkitelli Sipariş'
        ]
        
        for col in empty_cols:
            new_df[col] = '-'
        
        # 11. Dinamik ay başlıkları (5 kere yan yana)
        current_month = datetime.datetime.now().month
        months = ['Ocak', 'Şubat', 'Mart', 'Nisan', 'Mayıs', 'Haziran',
                 'Temmuz', 'Ağustos', 'Eylül', 'Ekim', 'Kasım', 'Aralık']
        
        next_month1 = months[(current_month) % 12]
        next_month2 = months[(current_month + 1) % 12]
        
        # 5 kere yan yana ay başlıkları
        for i in range(5):
            new_df[f'{next_month1}_{i+1}'] = '-'
            new_df[f'{next_month2}_{i+1}'] = '-'
        
        # 12. Diğer sütunlar
        other_cols = {
            'TOPL.FAT.ADT': 'TOPL.FAT.ADT',
            'MÜŞT.SAY.': 'MÜŞT.SAY.',
            'SATıŞ FIYATı': 'SATıŞ FIYATı',
            'DÖVIZ CINSI (S)': 'DÖVIZ CINSI (S)'
        }
        
        for old, new in other_cols.items():
            if old in df_filtered.columns:
                new_df[new] = df_filtered[old].fillna('')
        
        # 13. URUNKODU (DÖVIZ CINSI'den sonra)
        new_df['URUNKODU_3'] = df_filtered['URUNKODU'].fillna('')
        
        # 14. Son boş sütunlar (görseldeki gibi - tam sıralama)
        # Görseldeki sıralama: Kampanya Tipi, not, İSK, PRİM, BÜTÇE, liste, TD SF, Toplam İsk, Net Fiyat Kampanyası
        new_df['Kampanya Tipi'] = '-'
        new_df['not'] = '-'
        new_df['İSK'] = '-'
        new_df['PRİM'] = '-'
        new_df['BÜTÇE'] = '-'
        new_df['liste'] = '-'
        new_df['TD SF'] = '-'
        new_df['Toplam İsk'] = '-'
        new_df['Net Fiyat Kampanyası'] = '-'
        
        # Sütun sıralamasını düzelt - görseldeki sıraya göre
        desired_order = [
            'URUNKODU', 'URUNKODU_2', 'Düzenlenmiş Ürün Kodu', 'ACIKLAMA', 'URETİCİKODU', 'ORJİNAL', 'ESKİKOD',
            'CAT1', 'CAT2', 'CAT3', 'CAT4', 'CAT5', 'CAT6', 'CAT7',
            # Depo kolonları (sıralama: MASLAK, İMES, İKİTELLİ, BOLU, ANKARA)
            'MASLAK DEVIR', 'MASLAK ALIŞ', 'MASLAK SATIS', 'MASLAK STOK',
            'İMES DEVIR', 'İMES ALIŞ', 'İMES SATIS', 'İMES STOK',
            'İKİTELLİ DEVIR', 'İKİTELLİ ALIŞ', 'İKİTELLİ SATIS', 'İKİTELLİ STOK',
            'BOLU DEVIR', 'BOLU ALIŞ', 'BOLU SATIS', 'BOLU STOK',
            'ANKARA DEVIR', 'ANKARA ALIŞ', 'ANKARA SATIS', 'ANKARA STOK',
            # Boş sütunlar
            'Not', 'Maslak Depo Bakiye', 'Bolu Depo Bakiye', 'İmes Depo Bakiye',
            'Ankara Depo Bakiye', 'İkitelli Depo Bakiye', 'Kampanya Tipi',
            'Toplam İsk', 'Toplam Depo Bakiye', 'Maslak Tedarikçi Bakiye',
            'Bolu Tedarikçi Bakiye', 'İmes Tedarikçi Bakiye',
            'Ankara Tedarikçi Bakiye', 'İkitelli Tedarikçi Bakiye',
            'Paket Adetleri', 'Maslak Sipariş', 'Bolu Sipariş', 'İmes Sipariş', 'Ankara Sipariş', 'İkitelli Sipariş',
            # Ay başlıkları
            'Ağustos_1', 'Eylül_1', 'Ağustos_2', 'Eylül_2', 'Ağustos_3', 'Eylül_3', 'Ağustos_4', 'Eylül_4', 'Ağustos_5', 'Eylül_5',
            # Diğer sütunlar
            'TOPL.FAT.ADT', 'MÜŞT.SAY.', 'SATıŞ FIYATı', 'DÖVIZ CINSI (S)',
            'URUNKODU_3',
            # Son başlıklar (görseldeki sırayla)
            'Kampanya Tipi', 'not', 'İSK', 'PRİM', 'BÜTÇE', 'liste', 'TD SF', 'Toplam İsk', 'Net Fiyat Kampanyası'
        ]
        
        # Mevcut sütunları filtrele ve sırala
        available_cols = [col for col in desired_order if col in new_df.columns]
        if len(available_cols) > 0:
            new_df = new_df[available_cols]
        
        return new_df
    
    except Exception as e:
        st.error(f"Dönüşüm hatası: {str(e)}")
        return pd.DataFrame()

@st.cache_data(show_spinner="Marka eşleştirme yapılıyor...")
def match_brands_with_excel(main_df, uploaded_files):
    """CAT4 kolonundaki markalarla yüklenen Excel dosyalarını eşleştirir"""
    try:
        # Marka-Excel eşleştirme sözlüğü (bilgileri vereceğinizde güncellenecek)
        brand_excel_mapping = {
            'Schaeffler': 'excel1',
            'ZF': 'excel2', 
            'Delphi': 'excel3',
            'Valeo': 'excel4',
            'Filtron': 'excel5',
            'Mann': 'excel6'
        }
        
        # Ana DataFrame'i kopyala
        result_df = main_df.copy()
        
        # CAT4 kolonunu kontrol et
        if 'CAT4' not in main_df.columns:
            st.warning("CAT4 kolonu bulunamadı!")
            return main_df
        
        # Her marka için işlem yap
        for brand, excel_key in brand_excel_mapping.items():
            if excel_key in uploaded_files and uploaded_files[excel_key] is not None:
                try:
                    # Excel dosyasını oku
                    brand_df = pd.read_excel(uploaded_files[excel_key])
                    st.success(f"✅ {brand} verisi yüklendi: {len(brand_df)} satır")
                    
                    # Burada marka eşleştirme işlemi yapılacak
                    # Bilgileri verdiğinizde detaylandırılacak
                    
                except Exception as e:
                    st.error(f"❌ {brand} dosyası okuma hatası: {str(e)}")
        
        return result_df
        
    except Exception as e:
        st.error(f"Marka eşleştirme hatası: {str(e)}")
        return main_df

@st.cache_data(show_spinner="Excel oluşturuluyor...")
def format_excel(df):
    """Basit Excel oluşturma - mavi başlık yok"""
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Rapor')
            
            # Basit formatlama
            workbook = writer.book
            worksheet = writer.sheets['Rapor']
            
            # Güvenli genişlik ayarı
            for i, col in enumerate(df.columns):
                try:
                    # Sütun verilerinin maksimum uzunluğunu hesapla
                    col_data = df[col].astype(str)
                    max_data_len = col_data.str.len().max()
                    if pd.isna(max_data_len):
                        max_data_len = 0
                    
                    # Başlık uzunluğu
                    header_len = len(str(col))
                    
                    # Maksimum uzunluk
                    max_len = max(max_data_len, header_len) + 2
                    worksheet.set_column(i, i, max_len)
                except Exception:
                    # Hata durumunda sabit genişlik
                    worksheet.set_column(i, i, 15)
        
        output.seek(0)
        return output.getvalue()
    
    except Exception as e:
        # Hata durumunda basit Excel oluştur
        output = BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        return output.getvalue()

# Ana uygulama
def main():
    # Dosya yükleme alanı
    with st.expander("📤 ANA EXCEL DOSYASINI YÜKLEYİN", expanded=True):
        uploaded_file = st.file_uploader(
            "Excel dosyasını seçin (XLSX/XLS)",
            type=['xlsx', 'xls'],
            key="main_file"
        )
    
    if uploaded_file:
        try:
            with st.spinner("Dosya işleniyor, lütfen bekleyin..."):
                df = load_data(uploaded_file)
                st.success(f"✅ Yüklendi: {len(df):,} satır | {len(df.columns)} sütun")
                
                # Dönüşüm
                transformed_df = transform_data(df)
                
                # İndirme butonu
                if transformed_df is not None and len(transformed_df) > 0:
                    try:
                        excel_data = format_excel(transformed_df)
                        st.download_button(
                            label=f"📥 Dönüştürülmüş Veriyi İndir ({len(transformed_df):,} satır)",
                            data=excel_data,
                            file_name=f"donusturulmus_veri_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary"
                        )
                    except Exception as e:
                        st.error(f"Excel oluşturma hatası: {str(e)}")
                else:
                    st.warning("Dönüştürülecek veri bulunamadı.")
        
        except Exception as e:
            st.error(f"❌ Hata: {str(e)}")
            st.stop()
    
    # 7 farklı Excel ekleme kutusu
    st.header("📂 Ek Excel Dosyalarını Yükleme")
    st.write("Aşağıdaki 7 Excel dosyasını yükleyin:")
    
    # 7 Excel dosyası yükleme
    col1, col2 = st.columns(2)
    
    with col1:
        excel1 = st.file_uploader("Schaeffler Luk", type=['xlsx', 'xls'], key="excel1")
        excel2 = st.file_uploader("ZF İthal Bakiye", type=['xlsx', 'xls'], key="excel2")
        excel3 = st.file_uploader("Delphi Bakiye", type=['xlsx', 'xls'], key="excel3")
        excel4 = st.file_uploader("ZF Yerli Bakiye", type=['xlsx', 'xls'], key="excel4")
    
    with col2:
        excel5 = st.file_uploader("Valeo Bakiye", type=['xlsx', 'xls'], key="excel5")
        excel6 = st.file_uploader("Filtron Bakiye", type=['xlsx', 'xls'], key="excel6")
        excel7 = st.file_uploader("Mann Bakiye", type=['xlsx', 'xls'], key="excel7")
    
    # Yükleme kontrolü
    uploaded_files = {
        'excel1': excel1, 'excel2': excel2, 'excel3': excel3, 'excel4': excel4,
        'excel5': excel5, 'excel6': excel6, 'excel7': excel7
    }
    uploaded_count = sum(1 for file in uploaded_files.values() if file is not None)
    
    st.write(f"**Yüklenen dosya sayısı:** {uploaded_count}/7")
    
    # Güncelle butonu
    if uploaded_count > 0:
        if st.button("Marka Eşleştirme Yap", type="primary"):
            if 'transformed_df' in locals():
                # Marka eşleştirme işlemi
                final_df = match_brands_with_excel(transformed_df, uploaded_files)
                st.success(f"✅ Marka eşleştirme tamamlandı! {len(final_df)} satır işlendi.")
                
                # Final Excel indirme butonu
                if len(final_df) > 0:
                    try:
                        final_excel_data = format_excel(final_df)
                        st.download_button(
                            label=f"📥 Eşleştirilmiş Veriyi İndir ({len(final_df):,} satır)",
                            data=final_excel_data,
                            file_name=f"eslestirilmis_veri_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary"
                        )
                    except Exception as e:
                        st.error(f"Final Excel oluşturma hatası: {str(e)}")
            else:
                st.warning("Önce ana Excel dosyasını yükleyin ve dönüştürün.")
    else:
        st.info("Lütfen en az bir marka dosyası yükleyin.")
    
    # Ana sayfaya dönüş
    st.markdown("---")
    if st.button("🏠 Ana Sayfaya Dön", type="secondary"):
        st.switch_page("Home")

# Sidebar
def sidebar():
    st.sidebar.header("⚙️ Ayarlar")
    
    if st.sidebar.checkbox("Performans Modu (Deneysel)", False):
        st.session_state.perf_mode = True
        st.sidebar.warning("Bazı formatlamalar devre dışı bırakılacak")
    else:
        st.session_state.perf_mode = False
    
    st.sidebar.header("📋 Kurallar")
    st.sidebar.write("- 0 değerleri → '-' olarak değiştirilir")
    st.sidebar.write("- Depo önekleri yeni isimlere dönüştürülür")
    st.sidebar.write("- Kategori sütunları korunur")
    
    st.sidebar.header("ℹ️ Bilgi")
    st.sidebar.write(f"Son Güncelleme: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}")

if __name__ == "__main__":
    sidebar()
    main() 