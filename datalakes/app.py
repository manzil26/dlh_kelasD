from flask import Flask, render_template
import psycopg2
import matplotlib.pyplot as plt
import io
import base64
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from collections import defaultdict
from matplotlib.ticker import MaxNLocator



app = Flask(__name__)

def get_dw_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="adventureworksDw",
        user="postgres",
        password="dataEngginer"
    )

@app.route("/")
def dashboard():
    return render_template("dashboard.html", title="Dashboard Utama")




def fig_to_base64(fig):
    fig.patch.set_alpha(0.0)
    for ax in fig.axes:
        ax.set_facecolor('none')
        ax.tick_params(colors='white')
        for spine in ax.spines.values():
            spine.set_edgecolor('white')
        ax.yaxis.label.set_color('white')
        ax.xaxis.label.set_color('white')
        if ax.get_legend():
            ax.legend().get_frame().set_facecolor('none')
            ax.legend().get_frame().set_edgecolor('white')
            for text in ax.legend().get_texts():
                text.set_color('white')
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True)
    buf.seek(0)
    encoded = base64.b64encode(buf.getvalue()).decode('utf-8')
    plt.close(fig)
    return encoded

# Custom styling and encoding function
def fig_to_base64(fig):
    fig.patch.set_alpha(0.0)
    for ax in fig.axes:
        ax.set_facecolor('none')
        ax.tick_params(colors='white')
        ax.xaxis.label.set_color('white')
        ax.yaxis.label.set_color('white')
        for spine in ax.spines.values():
            spine.set_edgecolor('white')
        if ax.get_legend():
            ax.legend().get_frame().set_facecolor('none')
            ax.legend().get_frame().set_edgecolor('white')
            for text in ax.legend().get_texts():
                text.set_color('white')
    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True)
    buf.seek(0)
    encoded = base64.b64encode(buf.getvalue()).decode('utf-8')
    plt.close(fig)
    return encoded

@app.route("/txt")
def txt_analysis():
    conn = get_dw_connection()
    cur = conn.cursor()

    # Komentar Positif, Netral, Negatif
    cur.execute("""
        SELECT s.sentiment_label, SUM(f.post_count)
        FROM fact_social_sentiment f
        JOIN dim_sentiment s ON f.sentiment_key = s.sentiment_key
        GROUP BY s.sentiment_label
    """)
    sentiment_data = dict(cur.fetchall())
    positif = sentiment_data.get("Positif", 0)
    netral = sentiment_data.get("Netral", 0)
    negatif = sentiment_data.get("Negatif", 0)

    # 1. Pie Chart Distribusi Sentimen
    labels = list(sentiment_data.keys())
    values = list(sentiment_data.values())

    fig1, ax1 = plt.subplots(facecolor='none')  # transparan
    ax1.pie(
        values,
        labels=labels,
        autopct='%1.1f%%',
        startangle=90,
        colors=["#FF4E50", "#F9D423", "#A1C4FD"],
        textprops={'color': 'white'}  # --> agar teks berwarna putih
    )
    ax1.axis('equal')
    fig1.patch.set_alpha(0.0)  # buat background figure transparan

    pie_chart = fig_to_base64(fig1)


    # 2. Grouped Bar Chart Sentimen per Subkategori Produk
    cur.execute("""
        SELECT s.product_subcategory, s.sentiment_label, SUM(f.post_count)
        FROM fact_social_sentiment f
        JOIN dim_sentiment s ON f.sentiment_key = s.sentiment_key
        GROUP BY s.product_subcategory, s.sentiment_label
    """)
    rows = cur.fetchall()
    from collections import defaultdict
    grouped = defaultdict(lambda: {"Positif": 0, "Netral": 0, "Negatif": 0})
    for subcat, label, count in rows:
        grouped[subcat][label] += count
    subcats = list(grouped.keys())
    pos_vals = [grouped[k]["Positif"] for k in subcats]
    net_vals = [grouped[k]["Netral"] for k in subcats]
    neg_vals = [grouped[k]["Negatif"] for k in subcats]
    fig2, ax2 = plt.subplots(figsize=(10, 5), facecolor='none')
    x = range(len(subcats))
    ax2.bar(x, pos_vals, label="Positif", color="#FF4E50")
    ax2.bar(x, net_vals, bottom=pos_vals, label="Netral", color="#F9D423")
    bottom = [i + j for i, j in zip(pos_vals, net_vals)]
    ax2.bar(x, neg_vals, bottom=bottom, label="Negatif", color="#A1C4FD")
    ax2.set_xticks(x)
    ax2.set_xticklabels(subcats, rotation=45, ha='right', color='white')
    ax2.set_facecolor('none')
    fig2.patch.set_alpha(0.0)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['left'].set_color('white')
    ax2.spines['bottom'].set_color('white')
    ax2.tick_params(axis='x', colors='white')
    ax2.tick_params(axis='y', colors='white')
    ax2.legend(facecolor='none', edgecolor='white', labelcolor='white')
    grouped_chart = fig_to_base64(fig2)

    # 3. Bar Chart Jumlah Komentar per Platform (pakai gradasi)
    cur.execute("""
        SELECT p.platform_name, SUM(f.post_count)
        FROM fact_social_sentiment f
        JOIN dim_platform p ON f.platform_key = p.platform_key
        GROUP BY p.platform_name
    """)
    rows = cur.fetchall()
    platforms = [r[0] for r in rows]
    counts = [r[1] for r in rows]
    fig3, ax3 = plt.subplots(figsize=(8, 5), facecolor='none')
    cmap = LinearSegmentedColormap.from_list("custom_gradient", ["#FF4E50", "#F9D423"])
    colors = [cmap(i / len(counts)) for i in range(len(counts))]
    y_pos = np.arange(len(platforms))
    ax3.barh(y_pos, counts, color=colors)
    ax3.set_yticks(y_pos)
    ax3.set_yticklabels(platforms, color='white')
    ax3.set_xlabel("Jumlah Komentar", color='white')
    ax3.set_facecolor('none')
    fig3.patch.set_alpha(0.0)
    ax3.spines['bottom'].set_color('white')
    ax3.spines['left'].set_color('white')
    ax3.spines['top'].set_visible(False)
    ax3.spines['right'].set_visible(False)
    ax3.tick_params(axis='x', colors='white')
    ax3.tick_params(axis='y', colors='white')
    ax3.invert_yaxis()
    plt.tight_layout()
    platform_chart = fig_to_base64(fig3)

    # 4. Stacked Bar Chart Sentimen antar Platform
    cur.execute("""
        SELECT p.platform_name, s.sentiment_label, SUM(f.post_count)
        FROM fact_social_sentiment f
        JOIN dim_platform p ON f.platform_key = p.platform_key
        JOIN dim_sentiment s ON f.sentiment_key = s.sentiment_key
        GROUP BY p.platform_name, s.sentiment_label
    """)
    rows = cur.fetchall()
    stacked = defaultdict(lambda: {"Positif": 0, "Netral": 0, "Negatif": 0})
    for plat, label, count in rows:
        stacked[plat][label] += count
    plats = list(stacked.keys())
    pos_vals = [stacked[k]["Positif"] for k in plats]
    net_vals = [stacked[k]["Netral"] for k in plats]
    neg_vals = [stacked[k]["Negatif"] for k in plats]
    fig4, ax4 = plt.subplots(figsize=(10, 5), facecolor='none')
    x = range(len(plats))
    ax4.bar(x, pos_vals, label="Positif", color="#FF4E50")
    ax4.bar(x, net_vals, bottom=pos_vals, label="Netral", color="#F9D423")
    bottom = [i + j for i, j in zip(pos_vals, net_vals)]
    ax4.bar(x, neg_vals, bottom=bottom, label="Negatif", color="#A1C4FD")
    ax4.set_xticks(x)
    ax4.set_xticklabels(plats, rotation=45, ha='right', color='white')
    ax4.set_facecolor('none')
    fig4.patch.set_alpha(0.0)
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.spines['left'].set_color('white')
    ax4.spines['bottom'].set_color('white')
    ax4.tick_params(axis='x', colors='white')
    ax4.tick_params(axis='y', colors='white')
    ax4.legend(facecolor='none', edgecolor='white', labelcolor='white')
    stacked_chart = fig_to_base64(fig4)

    # 5. Line Chart Tren Sentimen per Tahun

    cur.execute("""
        SELECT t.year, s.sentiment_label, SUM(f.post_count)
        FROM fact_social_sentiment f
        JOIN dim_time_txt t ON f.time_id = t.time_id
        JOIN dim_sentiment s ON f.sentiment_key = s.sentiment_key
        GROUP BY t.year, s.sentiment_label
        ORDER BY t.year
    """)
    rows = cur.fetchall()
    years = sorted(set(r[0] for r in rows))
    trend = defaultdict(lambda: [0] * len(years))
    year_index = {y: i for i, y in enumerate(years)}
    for year, label, count in rows:
        trend[label][year_index[year]] = count

    # Ukuran diperbesar agar lebar (figsize)
    fig5, ax5 = plt.subplots(figsize=(12, 5), facecolor='none')

    for label, series in trend.items():
        ax5.plot(years, series, label=label, marker='o')

    # Styling agar tema dark dan garis bersih
    ax5.set_facecolor('none')
    fig5.patch.set_alpha(0.0)
    ax5.set_xlabel("Tahun", color='white')
    ax5.set_ylabel("Jumlah", color='white')
    ax5.tick_params(axis='x', colors='white')
    ax5.tick_params(axis='y', colors='white')
    ax5.spines['top'].set_visible(False)
    ax5.spines['right'].set_visible(False)
    ax5.spines['left'].set_color('white')
    ax5.spines['bottom'].set_color('white')
    plt.xticks(rotation=45)

    # ✅ Biar angka tahun tanpa koma
    from matplotlib.ticker import MaxNLocator
    ax5.xaxis.set_major_locator(MaxNLocator(integer=True))

    # Legend dan convert
    ax5.legend(facecolor='none', edgecolor='white', labelcolor='white')
    fig5.subplots_adjust(right=0.85)
    trend_chart = fig_to_base64(fig5)


    cur.close()
    conn.close()

    return render_template("txt.html",
        title="Analisis Sentimen TXT",
        positif=positif,
        netral=netral,
        negatif=negatif,
        pie_chart=pie_chart,
        grouped_chart=grouped_chart,
        platform_chart=platform_chart,
        stacked_chart=stacked_chart,
        trend_chart=trend_chart
    )

@app.route("/pdf")
def pdf_analysis():
    conn = get_dw_connection()
    cur = conn.cursor()

    # 1. Grafik Pendapatan Perusahaan (tanpa tahun)
    cur.execute("""
        SELECT c.company_name, SUM(f.revenue) AS revenue
        FROM fact_financial_report f
        JOIN dim_company c ON f.company_id = c.company_id
        GROUP BY c.company_name
        ORDER BY revenue DESC
    """)
    kinerja_data = cur.fetchall()

    # 2. Produk Terlaris (Top 10)
    cur.execute(""" 
       SELECT pc.best_selling_product, c.company_name, SUM(f.revenue) AS total_revenue
        FROM fact_financial_report f
        JOIN dim_product_competitor pc ON f.product_id = pc.product_id
        JOIN dim_company c ON f.company_id = c.company_id
        WHERE pc.is_current = TRUE
        GROUP BY pc.best_selling_product, c.company_name
        ORDER BY total_revenue DESC
        LIMIT 5
        """)
    produk_terlaris = cur.fetchall()


    # 3. Segmentasi Pasar
    cur.execute("""
        SELECT s.market_segment, SUM(f.revenue) AS total_revenue
        FROM fact_financial_report f
        JOIN dim_segment s ON f.segment_id = s.segment_id
        GROUP BY s.market_segment
        ORDER BY total_revenue DESC
    """)
    segmentasi_data = cur.fetchall()

    # 4. Lokasi Tertinggi
    cur.execute("""
        SELECT l.province_name, SUM(f.revenue) AS total_revenue
        FROM fact_financial_report f
        JOIN dim_location l ON f.location_id = l.location_id
        GROUP BY l.province_name
        ORDER BY total_revenue DESC
        LIMIT 5
    """)
    lokasi_data = cur.fetchall()

    # 5. Tren Kinerja Tahunan
    cur.execute("""
        SELECT d.year, SUM(f.revenue) AS total_revenue, SUM(f.profit) AS total_profit
        FROM fact_financial_report f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year
        ORDER BY d.year
    """)
    tren_data = cur.fetchall()

    # Insight
    cur.execute("""SELECT pc.best_selling_product FROM fact_financial_report f
        JOIN dim_product_competitor pc ON f.product_id = pc.product_id
        WHERE pc.is_current = TRUE GROUP BY pc.best_selling_product
        ORDER BY SUM(f.revenue) DESC LIMIT 1""")
    top_product = cur.fetchone()[0]

    cur.execute("""SELECT s.market_segment FROM fact_financial_report f
        JOIN dim_segment s ON f.segment_id = s.segment_id
        GROUP BY s.market_segment ORDER BY SUM(f.revenue) DESC LIMIT 1""")
    top_segment = cur.fetchone()[0]

    cur.execute("""SELECT l.province_name FROM fact_financial_report f
        JOIN dim_location l ON f.location_id = l.location_id
        GROUP BY l.province_name ORDER BY SUM(f.revenue) DESC LIMIT 1""")
    top_location = cur.fetchone()[0]

    cur.close()
    conn.close()

    # Generate chart
    chart_kinerja = generate_grouped_bar_chart_kinerja(kinerja_data)
    chart_produk = generate_grouped_bar_chart_produk_perusahaan(produk_terlaris)
    chart_segmentasi = generate_pie_chart(segmentasi_data)
    chart_lokasi = generate_bar_chart(lokasi_data, "Provinsi", "Pendapatan")
    chart_tren = generate_line_chart_tahunan(tren_data)

    return render_template("pdf.html",
                           title="Analisis Laporan Keuangan PDF",
                           top_product=top_product,
                           top_segment=top_segment,
                           top_location=top_location,
                           chart_kinerja=chart_kinerja,
                           chart_produk=chart_produk,
                           chart_segmentasi=chart_segmentasi,
                           chart_lokasi=chart_lokasi,
                           chart_tren=chart_tren)

# =================== CHART FUNCTIONS ===================

def generate_grouped_bar_chart_kinerja(data):
    df = pd.DataFrame(data, columns=['company', 'revenue'])

    companies = df['company']
    revenues = df['revenue']

    x = np.arange(len(companies))
    fig, ax = plt.subplots(figsize=(10, 5), facecolor='none')

    cmap = LinearSegmentedColormap.from_list("orange_red", ["#FF4E50", "#F9D423"])
    colors = [cmap(i / len(revenues)) for i in range(len(revenues))]

    ax.bar(x, revenues, color=colors)

    ax.set_xticks(x)
    ax.set_xticklabels(companies, rotation=45, ha='right', color='white')
    ax.set_ylabel("Pendapatan", color='white')
    ax.set_xlabel("Perusahaan", color='white')
    ax.set_title("Total Pendapatan per Perusahaan", color='white')

    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    for spine in ax.spines.values():
        spine.set_color('white')

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=True)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def generate_grouped_bar_chart_produk_perusahaan(data):
   

    df = pd.DataFrame(data, columns=['product', 'company', 'revenue'])

    df_top10 = df.sort_values(by='revenue', ascending=False).head(10).copy()

    perusahaan_total = (
        df.groupby('company')['revenue']
        .sum()
        .reindex(df_top10['company'])
        .reset_index(drop=True)
    )

    x = np.arange(len(df_top10))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6), facecolor='none')

    orange = "#FF4E50"
    yellow = "#F9D423"

    bars1 = ax.bar(x - width/2, df_top10['revenue'], width, label='Produk', color=orange)
    bars2 = ax.bar(x + width/2, perusahaan_total, width, label='Perusahaan', color=yellow)

    xtick_labels = []
    for i in range(len(df_top10)):
        xtick_labels.append(df_top10.iloc[i]['product'])
        xtick_labels.append(df_top10.iloc[i]['company'])

    xticks = []
    for i in x:
        xticks.append(i - width/2)
        xticks.append(i + width/2)

    ax.set_xticks(xticks)
    ax.set_xticklabels(xtick_labels, rotation=45, ha='right', color='white', fontsize=9)

    # Tidak ada judul
    ax.set_ylabel("", color='white')
    ax.set_xlabel("", color='white')

    ax.legend(facecolor='none', edgecolor='white', labelcolor='white')
    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    for spine in ax.spines.values():
        spine.set_color('white')

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=True)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')

def generate_pie_chart(data):
    labels = [row[0] for row in data]
    values = [row[1] for row in data]

    cmap = LinearSegmentedColormap.from_list("grad", ["#FF4E50", "#F9D423"])
    colors = [cmap(i / len(values)) for i in range(len(values))]

    fig, ax = plt.subplots(figsize=(6, 6), facecolor='none')
    ax.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, textprops={'color': 'white'})
    ax.axis('equal')
    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)

    buf = io.BytesIO()
    fig.savefig(buf, format='png', transparent=True)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


def generate_bar_chart(data, xlabel, ylabel):
    labels = [row[0] for row in data]
    values = [row[1] for row in data]

    fig, ax = plt.subplots(figsize=(10, 5), facecolor='none')
    cmap = LinearSegmentedColormap.from_list("custom_gradient", ["#FF4E50", "#F9D423"])
    colors = [cmap(i / len(values)) for i in range(len(values))]

    ax.bar(labels, values, color=colors, edgecolor='white')
    ax.set_xlabel(xlabel, color='white')
    ax.set_ylabel(ylabel, color='white')
    ax.set_title(f"{ylabel} per {xlabel}", color='white')
    ax.tick_params(axis='x', colors='white', rotation=45)
    ax.tick_params(axis='y', colors='white')
    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    for spine in ax.spines.values():
        spine.set_color('white')

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=True)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


def generate_line_chart_tahunan(data):
    tahun = [str(row[0]) for row in data]
    revenues = [row[1] for row in data]
    profits = [row[2] for row in data]

    fig, ax = plt.subplots(figsize=(10, 5), facecolor='none')
    ax.plot(tahun, revenues, label='Pendapatan', color='#FF4E50', marker='o', linewidth=2)
    ax.plot(tahun, profits, label='Laba', color='#F9D423', marker='s', linewidth=2)

    ax.set_xlabel("Tahun", color='white')
    ax.set_ylabel("Jumlah", color='white')
    ax.set_title("Tren Pendapatan & Laba Tahunan", color='white')
    ax.legend(facecolor='none', edgecolor='white', labelcolor='white')
    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    for spine in ax.spines.values():
        spine.set_color('white')

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=True)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


@app.route("/csv")
def csv_analysis():
    conn = get_dw_connection()
    cur = conn.cursor()

    # 1. Top Produk dengan Kerusakan Tertinggi
    cur.execute("""
        SELECT p.productname, SUM(f.damaged_units) AS total_damaged, SUM(f.total_units) AS total_units
        FROM fact_inventory_conditions f
        JOIN dim_product p ON f.product_key = p.product_key
        GROUP BY p.productname
        ORDER BY total_damaged DESC
        LIMIT 10
    """)
    damage_data = cur.fetchall()



    # 2. Pengaruh Suhu terhadap Tingkat Kerusakan
    cur.execute("""
        SELECT f.storage_temp_c, SUM(f.damaged_units) AS total_damaged_units
        FROM fact_inventory_conditions f
        GROUP BY f.storage_temp_c
        ORDER BY total_damaged_units DESC
        LIMIT 10
    """)
    temp_damage = cur.fetchall()

    # 3. Distribusi Lokasi Gudang Berdasarkan Kerusakan
    cur.execute("""
        SELECT w.warehouse_name, SUM(f.damaged_units) AS total_damaged
        FROM fact_inventory_conditions f
        JOIN dim_warehouse w ON f.warehouse_sk = w.warehouse_sk
        GROUP BY w.warehouse_name
        ORDER BY total_damaged DESC
    """)
    warehouse_damage = cur.fetchall()
     # 5. Jumlah Gudang
    cur.execute("SELECT COUNT(DISTINCT warehouse_sk) FROM dim_warehouse")
    total_warehouses = cur.fetchone()[0]

    # 6. Total Barang Rusak
    cur.execute("SELECT SUM(damaged_units) FROM fact_inventory_conditions")
    total_damaged = cur.fetchone()[0]

       # 7. Total Barang Tidak Rusak
    cur.execute("SELECT SUM(total_units - damaged_units) FROM fact_inventory_conditions")
    total_non_damaged = cur.fetchone()[0]

   

    # 4. Tren Kerusakan dari Waktu ke Waktu
    cur.execute("""
        SELECT t.year, t.month, SUM(f.damaged_units) AS total_damaged
        FROM fact_inventory_conditions f
        JOIN dim_time_warehouse t ON f.time_id = t.time_id
        GROUP BY t.year, t.month
        ORDER BY t.year, t.month
    """)

   

    trend_damage = cur.fetchall()

    cur.close()
    conn.close()

    # Generate all charts
    chart1 = generate_barh_chart(damage_data, "Produk")
    chart2 = generate_histogram(temp_damage, "Suhu (°C)", "Jumlah Rusak")
    chart3 = generate_bar_chart(warehouse_damage, "Lokasi Gudang", "Jumlah Rusak")
    chart4 = generate_line_chart(trend_damage, "Tahun", "Jumlah Rusak")


    return render_template("csv.html",
                           total_warehouses=total_warehouses,
                           total_damaged=total_damaged,
                           total_non_damaged=total_non_damaged,
                           damage_data=damage_data,
                           temp_damage=temp_damage,
                           warehouse_damage=warehouse_damage,
                           trend_damage=trend_damage,
                           chart1=chart1,
                           chart2=chart2,
                           chart3=chart3,
                           chart4=chart4,
                           title="Analisis Gudang CSV")




def generate_barh_chart(data, xlabel):
    names = [row[0] for row in data]
    values = [row[1] for row in data]
    y_pos = np.arange(len(names))

    colors = np.linspace(0, 1, len(values))
    cmap = LinearSegmentedColormap.from_list("custom_gradient", ["#FF4E50", "#F9D423"])
    bar_colors = [cmap(c) for c in colors]

    fig, ax = plt.subplots(figsize=(8, 5), facecolor='none')
    ax.barh(y_pos, values, color=bar_colors)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, color='white')
    ax.set_xlabel(xlabel, color='white')

    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)

    ax.spines['bottom'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')

    ax.invert_yaxis()
    plt.tight_layout()
    return convert_plot_to_base64(fig)

def generate_histogram(data, xlabel, ylabel):
    df = pd.DataFrame(data, columns=['temp', 'damaged'])
    bins = np.arange(int(df['temp'].min()), int(df['temp'].max()) + 2, 2)
    df['temp_bin'] = pd.cut(df['temp'], bins)
    grouped = df.groupby('temp_bin')['damaged'].sum().reset_index()

    labels = grouped['temp_bin'].astype(str)
    values = grouped['damaged']

    fig, ax = plt.subplots(figsize=(10, 5), facecolor='none')
    cmap = LinearSegmentedColormap.from_list("custom_gradient", ["#FF4E50", "#F9D423"])
    colors = [cmap(i / len(values)) for i in range(len(values))]
    ax.bar(labels, values, color=colors, edgecolor='white')

    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    ax.set_xlabel(xlabel, color='white')
    ax.set_ylabel(ylabel, color='white')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax.set_xticklabels(labels, rotation=45, ha='right')

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('white')
    ax.spines['bottom'].set_color('white')

    for i, v in enumerate(values):
        ax.text(i, v + 1, str(v), ha='center', va='bottom', fontsize=9, color='white')

    plt.tight_layout()
    return convert_plot_to_base64(fig)

def generate_bar_chart(data, xlabel, ylabel):
    labels = [row[0] for row in data]
    values = [row[1] for row in data]

    fig, ax = plt.subplots(figsize=(8, 5), facecolor='none')
    cmap = LinearSegmentedColormap.from_list("custom_gradient", ["#FF4E50", "#F9D423"])
    colors = [cmap(i / len(values)) for i in range(len(values))]
    ax.bar(labels, values, color=colors, edgecolor='white')

    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    ax.set_xlabel(xlabel, color='white')
    ax.set_ylabel(ylabel, color='white')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('white')
    ax.spines['bottom'].set_color('white')
    plt.xticks(rotation=45, ha='right')

    plt.tight_layout()
    return convert_plot_to_base64(fig)

def generate_line_chart(data, xlabel, ylabel):
    labels = [str(row[0]) for row in data]
    values = [row[2] for row in data]

    fig, ax = plt.subplots(figsize=(10, 5), facecolor='none')
    ax.plot(labels, values, marker='o', linestyle='-', color='#F9D423')

    ax.set_facecolor('none')
    fig.patch.set_alpha(0.0)
    ax.set_xlabel(xlabel, color='white')
    ax.set_ylabel(ylabel, color='white')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('white')
    ax.spines['bottom'].set_color('white')
    plt.xticks(rotation=45)

    plt.tight_layout()
    return convert_plot_to_base64(fig)

def convert_plot_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', transparent=True)
    buf.seek(0)
    image_base64 = base64.b64encode(buf.read()).decode('utf-8')
    buf.close()
    plt.close(fig)
    return image_base64


app.run(debug=True)
