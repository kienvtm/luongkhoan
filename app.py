import streamlit as st
import duckdb
import pandas as pd
import datetime
import pytz
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
st.set_page_config(layout="wide")


st.cache_data.clear()
# data folder path
cwd = Path(__file__).parent

# daily file
dta_daily_path = cwd.joinpath('data_luongtt.parquet')
tier_tc_path = cwd.joinpath('tier_tc.parquet')

db = duckdb.connect()
db.execute(f"CREATE or replace temp VIEW data_daily AS SELECT * FROM '{dta_daily_path}'")
db.execute(f"CREATE or replace temp VIEW tc_tier AS SELECT * FROM '{tier_tc_path}'")

st.title("Dashboard Lương khoán theo TC từng ngày")

@st.cache_data
def get_data_daily(from_date, to_date, store=''):
    if len(store)>0:
        query = rf'''
        SELECT 
            *
        FROM data_daily 
        WHERE store_vt in ({store})
        and report_date between '{from_date}' and '{to_date}'
        '''
    else:
        query = rf'''
        SELECT 
            *
        FROM data_daily 
        '''
    # st.write(query)
    df_data = db.execute(query).fetch_df()
    # st.write(len(df_data))
    return df_data

@st.cache_data
def get_tier_tc(store=''):
    if len(store)>0:
        query = rf'''
        SELECT 
            *
        FROM tc_tier 
        WHERE storevt in ({store})
        '''
    else:
        query = rf'''
        SELECT 
            *
        FROM tc_tier 
        '''
    # st.write(query)
    df_data = db.execute(query).fetch_df()
    # st.write(len(df_data))
    return df_data


@st.cache_data
def get_store(username):
    if username == 'admin':
        query = rf'''
        SELECT 
            distinct store_vt
        FROM data_daily
        order by store_vt
        '''
    else:
        query = rf'''
        SELECT 
            distinct store_vt
        FROM data_daily
        where right(profit_center, 4) = '{username}'
        order by store_vt
        '''
    # st.write(query)
    df_data = db.execute(query).fetch_df()
    return df_data


@st.cache_data
def get_dayofweek():

    query = rf'''
        SELECT 
            distinct day_of_week2
        FROM data_daily
        order by day_of_week2
        '''
    # st.write(query)
    df_data = db.execute(query).fetch_df()
    return df_data

def chart_luong_tt(data_daily):
    '''
    Chat the hien chenh lech giua Luong TT thuc te voi Luong khoan tinh theo moving avg TC MTD
    '''
    # Define colors for 'chenh_lech_luong_khoan'
    colors = ['#87FE1A' if val > 0 else '#F90202' for val in data_daily['chenh_lech_luong_khoan']]

    # Create a stacked bar chart using Plotly
    fig = go.Figure()

    # Lollipop chart for 'luong_tt_daily' and 'total_luongtt_act'
    for idx, row in data_daily.iterrows():
        # Line from 'total_luongtt_act' to 'luong_tt_daily'
        fig.add_trace(go.Scatter(
            x=[row['report_date'], row['report_date']],
            y=[0, row['luong_tt_daily']],
            mode='lines',
            line=dict(color='grey', width=0.5),
            showlegend=False
        ))
        # Line from 'total_luongtt_act' to 'luong_tt_daily'
        fig.add_trace(go.Scatter(
            x=[row['report_date'], row['report_date']],
            y=[0, row['total_luongtt_act']],
            mode='lines',
            line=dict(color='grey', width=0.5),
            showlegend=False
        ))
        # Point for 'total_luongtt_act'
        fig.add_trace(go.Scatter(
            x=[row['report_date']],
            y=[row['total_luongtt_act']],
            mode='markers',
            marker=dict(color='blue', size=10),
            hovertemplate=(
                "Date: %{x}<br>"
                "Actual: %{y:,.0f}<br>"
            ),
            name='Actual' if idx == 0 else None
        ))
        # Point for 'luong_tt_daily'
        fig.add_trace(go.Scatter(
            x=[row['report_date']],
            y=[row['luong_tt_daily']],
            mode='markers',
            marker=dict(color='orange', size=10),
            hovertemplate=(
                "Date: %{x}<br>"
                "Khoán: %{y:,.0f}<br>"
            ),
            name='Khoán' if idx == 0 else None
        ))


    fig.add_trace(go.Bar(
        x=data_daily['report_date'],
        y=data_daily['abs_chenh_lech'],
        name='Chenh Lech',
        marker_color=colors,
        opacity=0.8,
        base=data_daily['min_luong'],  # Stack on top of 'min_luong'
        text=data_daily['chenh_lech_luong_khoan']/1e6,  # Add data labels for 'abs_chenh_lech'
        texttemplate='%{text:,.2f}M',  # Format with comma as thousand separator and no decimals
        textposition='outside',
        hovertemplate=(
            "Date: %{x}<br>"
            "Actual: %{customdata[0]:,.0f}<br>"
            "Khoán: %{customdata[1]:,.0f}<br>"
            "Chênh lệch: %{customdata[2]:,.0f}<extra></extra>"
        ),
        customdata=data_daily[['total_luongtt_act', 'luong_tt_daily', 'chenh_lech_luong_khoan']]
    ))

    # Update layout
    fig.update_layout(
        title='Chênh lệch Khoán - Thực tế hàng ngày',
        # xaxis_title=False,
        # yaxis_title='Values',
        barmode='stack',
        showlegend=False,
        yaxis=dict(showgrid=False),  # Hide vertical grid lines
    )
    return fig


def chart_tc(data_daily):
    '''
    Ve chart cho TC actual vs TC forecast
    '''
    # Create a bar chart for 'tc_forecast' and 'tc'
    fig = go.Figure()

    # Add 'tc_forecast' as a bar
    fig.add_trace(go.Bar(
        x=data_daily['report_date'],
        y=data_daily['tc_forecast'],
        name='TC Forecast',
        marker_color='blue',
        text=data_daily['tc_forecast'],  # Add data labels for 'abs_chenh_lech'
        texttemplate='%{text:,.0f}',  # Format with comma as thousand separator and no decimals
        textposition='outside',
    ))

    # Add 'tc' as a bar
    fig.add_trace(go.Bar(
        x=data_daily['report_date'],
        y=data_daily['tc'],
        name='TC Actual',
        marker_color='orange',
        text=data_daily['tc'],  # Add data labels for 'abs_chenh_lech'
        texttemplate='%{text:,.0f}',  # Format with comma as thousand separator and no decimals
        textposition='outside',
    ))

    # Update layout
    fig.update_layout(
        title='TC Forecast and TC Actual',
        # xaxis_title='Report Date',
        # yaxis_title='Values',
        barmode='group',  # Display bars side by side
        showlegend=True,
        legend=dict(
            orientation='h',  
            x=0,  # Position legend at the left
            y=1.15,  # Position legend at the top
            xanchor='left',
            yanchor='top'
        ),
        yaxis=dict(showgrid=False, visible=False),  # Hide vertical grid lines
    )

    return fig

def chart_whr(data_daily):
    data_daily['pct_whr_gstar_to_baseline'] = (data_daily['whr_gstar'] / data_daily['baseline_rfc']) * 100
    data_daily['pct_whr_gstar_to_total_whr'] = (data_daily['whr_gstar'] / data_daily['total_whr_act']) * 100
    # Create the figure
    fig = go.Figure()

    # Add line for 'baseline_rfc'
    fig.add_trace(go.Scatter(
        x=data_daily['report_date'],
        y=data_daily['baseline_rfc'],
        mode='lines',
        name='Baseline TC RFC',
        line_shape='hvh',
        line=dict(color='red', width=2)
    ))
    
    # Add line for 'baseline_rfc'
    fig.add_trace(go.Scatter(
        x=data_daily['report_date'],
        y=data_daily['baseline_act'],
        mode='lines',
        name='Baseline TC Act',
        line_shape='hvh',
        line=dict(color='blue', width=2)
    ))

    # Add stacked bar for 'whr_act'
    fig.add_trace(go.Bar(
        x=data_daily['report_date'],
        y=data_daily['whr_act'],
        name='WHR GGG',
        marker_color='#C3DDEA'
    ))

    # Add stacked bar for 'whr_gstar'
    fig.add_trace(go.Bar(
        x=data_daily['report_date'],
        y=data_daily['whr_gstar'],
        name='WHR Gstar',
        marker_color='orange',
        hovertemplate=(
            "Report Date: %{x}<br>"
            "Gstar: %{y}<br>"
            "Gstar/Baseline RFC: %{customdata[0]:.2f}%<br>"
            "Gstar/Act: %{customdata[1]:.2f}%<br>"
        ),
        customdata=data_daily[['pct_whr_gstar_to_baseline', 'pct_whr_gstar_to_total_whr']]
    ))

    # Update layout for the stacked bar and line combination
    fig.update_layout(
        title='Giờ công',
        # xaxis_title='Report Date',
        # yaxis_title='Values',
        legend=dict(
                orientation='h',  
                x=0,  # Position legend at the left
                y=1.05,  # Position legend at the top
                xanchor='left',
                yanchor='bottom'
            ),
        barmode='stack',  # Stacking bars for 'whr_act' and 'whr_gstar'
        hovermode='x unified',  # Unified x-axis hover
        showlegend=True
    )
    return fig 

# Define a function to apply color formatting for 'chenh_lech_luong_khoan'
def highlight_chenh_lech(val):
    color = 'red' if val < 0 else 'green' if val > 0 else 'black'
    return f'color: {color}'

def highlight_row(row):
    color = 'background-color: #FF5353' if row['chenh_lech_luong_khoan'] < 0 else (
            'background-color: #67FF53' if row['chenh_lech_luong_khoan'] > 0 else '')
    return [color] * len(row)

# Define a function to apply text color based on 'chenh_lech_luong_khoan'
def highlight_text(row):
    color = '#FF5353' if row['Chênh lệch Khoán - Thực tế hàng ngày'] < 0 else (
            '#3CB32D' if row['Chênh lệch Khoán - Thực tế hàng ngày'] > 0 else 'black')
    return [f'color: {color}' for _ in row]

def display_table(data_daily):
    # Store original numeric values for formatting and display
    
    cols = ['brand', 'store_vt', 
            'report_date', 
            'tc_forecast', 
        'baseline_rfc', 
        'whr_sche',
            'tc', 
        'baseline_act', 
        'whr_act', 
        'whr_gstar', 
        'total_whr_act',
            'luongtt_gstar',
            'luongtt_ggg',
            'total_luongtt_act', 
            # 'luong_tt_daily', 'mtd_avg_tc', 'chenh_lech_luong_khoan',
            'luong_tt_daily', 
            # 'moving_mtd_avg_tc', 
            'chenh_lech_luong_khoan',
        #    'tc_from_daily_mtd', 'tc_to_daily_mtd', 
        #    'luong_tt_tier0',
        #    'bonus_per_tc_over_avg_mtd', 'bonus_fix_daily_avg_mtd',       'bonus_daily_avg_mtd', 
        ]
    data_table = data_daily[cols]
    data_table['whr_act_vs_baseline_act'] = (data_table['total_whr_act'] / data_table['baseline_act']) * 100 - 100
    # Format 'report_date' to show only year-month-date
    data_table['report_date'] = data_table['report_date'].dt.strftime('%Y-%m-%d')
    data_display = data_table.copy()

    rename_cols ={'brand':'Brand', 
                'store_vt':'Store', 
                'tc_forecast':'TC Forecast', 
                'report_date':'Date', 
                'tc':'TC Actual', 
                'luongtt_gstar':'Lương Gstar',
                'luongtt_ggg':'Lương trực tiếp',
                'total_luongtt_act':'Tổng lương trực tiếp', 
                # 'luong_tt_daily':'Lương khoán theo TC từng ngày', 
                # 'mtd_avg_tc':'TC trung bình', 
                # 'chenh_lech_luong_khoan':'Chênh lệch Khoán - Thực tế hàng ngày',
                'luong_tt_daily':'Lương khoán theo TC từng ngày', 
                # 'moving_mtd_avg_tc':'TC trung bình', 
                'chenh_lech_luong_khoan':'Chênh lệch Khoán - Thực tế hàng ngày',
                # 'tc_from_daily_mtd', 'tc_to_daily_mtd', 'luong_tt_tier0',
                # 'bonus_per_tc_over_avg_mtd', 'bonus_fix_daily_avg_mtd',
                # 'bonus_daily_avg_mtd', 
                'whr_sche':'Giờ công lập lịch',
                'baseline_act':'Baseline TC Actual', 
                'baseline_rfc':'Baseline TC Forecast', 
                'whr_act':'Giờ công thực tế', 
                'whr_gstar':'Giờ công Gstar', 
                'total_whr_act':'Tổng giờ công',
                'whr_act_vs_baseline_act':'Chênh lệch WHR Thực tế - Baseline TC Act (%)'
    }

    format_cols2 =['TC Forecast', 'TC Actual', 'Lương Gstar', 'Lương trực tiếp','Tổng lương trực tiếp','Lương khoán theo TC từng ngày','Chênh lệch Khoán - Thực tế hàng ngày',
                #    'TC trung bình',
                   'Giờ công lập lịch','Baseline TC Actual','Baseline TC Forecast','Giờ công thực tế', 'Giờ công Gstar', 'Tổng giờ công', 'Chênh lệch WHR Thực tế - Baseline TC Act (%)' ]
    
    data_display.rename(columns=rename_cols, inplace=True)

    # summary theo tong va avg
    data_display_sum = data_display.drop(columns='Date').groupby(['Brand', 'Store']).sum()
    data_display_sum['Chênh lệch WHR Thực tế - Baseline TC Act (%)'] = (data_display_sum['Tổng giờ công'] / data_display_sum['Baseline TC Actual']) * 100 - 100
    data_display_sum['aggregation'] = 'Sum Total'
    data_display_avg = data_display.drop(columns='Date').groupby(['Brand', 'Store']).mean()
    data_display_avg['Chênh lệch WHR Thực tế - Baseline TC Act (%)'] = (data_display_avg['Tổng giờ công'] / data_display_avg['Baseline TC Actual']) * 100 - 100
    data_display_avg['aggregation'] = 'Average Total'
    summary_data = pd.concat(objs=[data_display_sum, data_display_avg], axis=0)
    # Move the last column to the first position
    last_col = summary_data.columns[-1]
    summary_data = summary_data[[last_col] + summary_data.columns[:-1].tolist()]

    styled_data = data_display.style.format(subset=format_cols2, formatter="{:,.0f}").apply(highlight_text, axis=1)
    styled_data_summary = summary_data.sort_index().reset_index().style.format(subset=format_cols2, formatter="{:,.0f}").apply(highlight_text, axis=1)
    return styled_data, styled_data_summary

def display_tiertc(tier_tc):

    tier_tc['luong_tt_tier0_monthly'] = tier_tc['luong_tt_tier0']*30
    cols = [
        'brand', 'pc', 'storevt', 'level_report',
       'tc_from_daily', 
       'tc',
       'tier_from', 
       'tier_monthly',
       'luong_tt_tier0',
       'luong_tt_tier0_monthly', 
        'bonus_per_tc_over'
       ]
    rename_cols = {
        'brand':"Brand", 
        'pc':"Profit center", 
        'storevt':"Store", 
        'level_report':"Level",
        'tc_from_daily':"TC/ngày từ", 
        'tc':"TC/ngày đến",
        'tier_from':"TC/tháng từ", 
        'tier_monthly':"TC/tháng đến",
        'luong_tt_tier0':"Lương cơ bản tại tier0/ngày", 
        'luong_tt_tier0_monthly':"Lương cơ bản tại tier0/tháng",
        'bonus_per_tc_over':"X-đơn giá tiền lương/TC"
    }
    format_cols = ["TC/ngày từ", "TC/ngày đến", "TC/tháng từ","TC/tháng đến","Lương cơ bản tại tier0/ngày","Lương cơ bản tại tier0/tháng","X-đơn giá tiền lương/TC"]

    data_table = tier_tc[cols].rename(columns=rename_cols)
    styled_data = data_table.style.format(subset=format_cols, formatter="{:,.0f}")

    return styled_data

def chart_dayofweek(box_data):
    fig1 = px.violin(box_data, y='Chênh lệch', x='Ngày trong tuần', 
                    points='all', box=True,
                    color='Ngày trong tuần',
                    hover_data={
                    'Date': True,
                    'Store':True, 
                    'profit_center': True,
                    'Chênh lệch': ':,.0f',  # Format with comma and one decimal place
                    'Lương thực tế': ':,.0f', 
                    'Lương khoán theo TC từng ngày': ':,.0f', 
                    'TC forecast':':,.0f', 
                    'TC Actual':':,.0f', 
                    },
                    )
    # Update layout for better visualization (optional)
    fig1.update_traces(marker=dict(opacity=0.7),
                    meanline_visible=True,
                    )  # Adjust marker opacity if needed
    fig1.update_layout(
        title='Chênh lệch Khoán - Thực tế hàng ngày theo ngày trong tuần',
        showlegend=False
        # xaxis_title='Brand',
        # yaxis_title='Avg luongtt per tc',
    )
    return fig1

def chart_store(box_data):
    # fig2 = px.box(data_daily, y='chenh_lech_luong_khoan', x='store_vt')
    fig2 = px.violin(box_data, y='Chênh lệch', x='Store', 
                    points='all', box=True,
                    color='Store',
                    hover_data={
                    'Date': True,
                    'Ngày trong tuần': True,
                    'Store':True, 
                    'profit_center': True,
                    'Chênh lệch': ':,.0f',  # Format with comma and one decimal place
                    'Lương thực tế': ':,.0f', 
                    'Lương khoán theo TC từng ngày': ':,.0f', 
                    'TC forecast':':,.0f', 
                    'TC Actual':':,.0f', 
                    },
                    )
    # Update layout for better visualization (optional)
    fig2.update_traces(marker=dict(opacity=0.7),
                    meanline_visible=True,
                    )  # Adjust marker opacity if needed
    fig2.update_layout(
        title='Chênh lệch Khoán - Thực tế hàng ngày theo từng nhà',
        showlegend=False
        # xaxis_title='Brand',
        # yaxis_title='Avg luongtt per tc',
    )
    return fig2


CREDENTIALS = st.secrets["credentials"]
# st.write(CREDENTIALS)
def login():
    st.sidebar.title("Login")
    username_input = st.sidebar.text_input("Username")
    password_input = st.sidebar.text_input("Password", type="password")

    if st.sidebar.button("Login"):
        for user, creds in CREDENTIALS.items():
            if username_input == creds["username"] and password_input == creds["password"]:
                st.write(creds)
                st.session_state["authenticated"] = True
                st.session_state["username"] = creds["username"]
                st.session_state['displayname'] = creds["displayname"]
                st.sidebar.success(f"Welcome, {creds['displayname']}!")
                st.rerun()
        st.sidebar.error("Invalid username or password")
    # return username_input, st.session_state['displayname']

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    login()
else:
    st.write(f"Welcome, {st.session_state['displayname']}!")
    if st.session_state["username"] == "admin":
        st.write("You have admin access.")
    else:
        st.write("You have user access.")

    # Logout button
    if st.sidebar.button('Logout'):
        st.session_state['authenticated'] = False
        st.session_state.pop('username', None)
        st.rerun()

    # Get the current date
    current_date = datetime.datetime.now()

    # Get the start of the current month
    start_of_month = current_date.replace(day=1)
    from_date = st.sidebar.date_input('Lay du lieu tu ngay', value=start_of_month)
    to_date = st.sidebar.date_input('Lay du lieu den ngay')

    # st.write(from_date)
    # st.write(to_date)

    stores = get_store(st.session_state["username"])
    stores = list(stores.sort_values(by='store_vt')['store_vt'])
    store_str = ','.join(["'"+x+"'" for x in list(stores)])

    # chon_vitri = st.sidebar.selectbox(label='Chon vi tri',
    #                                options=vitri,
    #                                )
    # if len(chon_vitri)==0:
    #     chon_vitri='total'

    chon_store = st.sidebar.multiselect(label='Chon store',
                                    options=stores,
                                    # default='GG Lê Trọng Tấn'
                                    )
    chon_store = ','.join(["'"+x+"'" for x in chon_store])
    if len(chon_store)==0:
        chon_store = store_str

    day_of_weeks = get_dayofweek()
    day_of_weeks = list(day_of_weeks['day_of_week2'])
    dayofweek_str = ','.join(["'"+x+"'" for x in day_of_weeks])
    chon_dayofweek = st.sidebar.multiselect(label='Chon ngay trong tuan',
                                    options=day_of_weeks,
                                    # default='GG Lê Trọng Tấn'
                                    )
    # chon_dayofweek = ','.join(["'"+x+"'" for x in chon_dayofweek])
    
    # st.write(chon_dayofweek)

    data_daily1 = get_data_daily(from_date, to_date, chon_store)
    if len(chon_dayofweek)==0:
        data_daily = data_daily1.copy()
    else:
        flt = data_daily1['day_of_week2'].isin(chon_dayofweek)
        data_daily = data_daily1[flt]

    last_update_time = data_daily['cob_dt'].max()

    # Define your local timezone (for example, 'Asia/Singapore')
    local_timezone = pytz.timezone('Etc/GMT-7')

    # Convert to your local timezone
    last_update_time_local = last_update_time.astimezone(local_timezone)
    st.write(last_update_time_local)

    tier_tc = get_tier_tc(chon_store)
    # st.dataframe(data_daily)

    data_daily['chenh_lech_luong_khoan'] = data_daily['luong_tt_daily'] - data_daily['total_luongtt_act']
    # data_daily['chenh_lech_luong_khoan'] = data_daily['luong_tt_daily'] - data_daily['total_luongtt_act']
    data_daily['min_luong'] = data_daily[['luong_tt_daily', 'total_luongtt_act']].min(axis=1)
    data_daily['abs_chenh_lech'] = data_daily['chenh_lech_luong_khoan'].abs()
    data_daily['pct_whr_gstar_to_baseline'] = (data_daily['whr_gstar'] / data_daily['baseline_rfc']) * 100
    data_daily['pct_whr_gstar_to_total_whr'] = (data_daily['whr_gstar'] / data_daily['total_whr_act']) * 100
    data_daily['whr_act_vs_baseline_act'] = (data_daily['total_whr_act'] / data_daily['baseline_act']) * 100

    format_cols = [
        'tc', 'mtd_avg_tc', 'tc_from_daily_mtd', 'tc_to_daily_mtd', 'luong_tt_tier0',
        'bonus_per_tc_over_avg_mtd', 'bonus_fix_daily_avg_mtd', 'bonus_daily_avg_mtd',
        'luong_tt_daily', 'whr_act', 'whr_sche', 'baseline_act', 'baseline_rfc',
        'whr_gstar', 'luongtt_gstar', 'total_whr_act','chenh_lech_luong_khoan',
        'luongtt_ggg', 'total_luongtt_act'
    ]

    for col in format_cols:
        data_daily[col] = pd.to_numeric(data_daily[col])



    # st.dataframe(data_daily[cols])


    # st.dataframe(data_daily.head())

    data_chart = data_daily.groupby(['report_date'], as_index=False).agg({'tc':'sum',
                                                                        'tc_forecast':'sum',
                                                                        'luong_tt_daily':'sum',
                                                                        'total_luongtt_act':'sum',
                                                                        'chenh_lech_luong_khoan':'sum',
                                                                        'abs_chenh_lech':'sum',
                                                                        'min_luong':'sum',
                                                                        'baseline_rfc':'sum',
                                                                        'baseline_act':'sum',
                                                                        'whr_act':'sum',
                                                                        'whr_gstar':'sum',
                                                                        'total_whr_act':'sum',
                                                                        })

    chart_luongtt = chart_luong_tt(data_chart)
    chart_tc = chart_tc(data_chart)


    chart_whr = chart_whr(data_chart)

    tong_khoan = data_daily['luong_tt_daily'].sum()
    tong_actual = data_daily['total_luongtt_act'].sum()
    chenh_lech = tong_khoan - tong_actual


    box_data = data_daily.rename(columns={
                    'report_date': 'Date',
                    'store_vt':'Store', 
                    'chenh_lech_luong_khoan': 'Chênh lệch',  # Format with comma and one decimal place
                    'total_luongtt_act': 'Lương thực tế', 
                    'luong_tt_daily': 'Lương khoán theo TC từng ngày', 
                    'tc_forecast':'TC forecast', 
                    'tc':'TC Actual', 
                    'day_of_week2':'Ngày trong tuần'
    })
    fig1 = chart_dayofweek(box_data)
    fig2 = chart_store(box_data)

    col01, col02 = st.columns(2)
    with col01:
        with st.container(border=True):
            col21, col22, col23 = st.columns(3)
            with col21:
                st.metric(label='Lương khoán theo TC từng ngày tạm tính', 
                        value=f'{tong_khoan/1e6:,.1f}M',
                        )
            with col22:
                st.metric(label='Lương thực tế tạm tính', 
                        value=f'{tong_actual/1e6:,.1f}M',
                        )
            with col23:
                st.metric(label='Chênh lệch', 
                        value=f'{chenh_lech/1e6:,.1f}M',
                        )
    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.plotly_chart(chart_luongtt)
        with st.container(border=True):
            st.plotly_chart(chart_tc)
    with col2:
        with st.container(border=True):
            st.plotly_chart(chart_whr)
        with st.container(border=True):
            st.plotly_chart(fig1)


    styled_data, styled_data_summary =display_table(data_daily)

    with st.expander("Dữ liệu tổng hợp"):
        st.dataframe(styled_data_summary)
        
    with st.expander("Dữ liệu chi tiết"):
    # Display the formatted DataFrame
        ghi_chu = '''
        **[1] Baseline Forecast**: giờ công do hệ thống Ghero tính toán dựa trên TC RFC  
        **[2] Giờ công lập lịch**: giờ công lập lịch trên Ghero do nhà hàng xếp lịch, lưu ý chỉ xếp tối đa 70% của [1]  
        **[3] Giờ công thực tế**: giờ công thực tế của nhân viên nhà hàng ghi nhận, tối đa chỉ tương đương với [2]  
        **[4] Giờ công Gstar**: giờ công trên Job market, tối đa bằng 30% của [1]  
        **[5] Tổng giờ công** = [3] Giờ công thực tế + [4] Giờ công Gstar  
        **[6] Baseline Actual**: giờ công do hệ thống Ghero tính toán dựa trên TC Actual
        '''
        st.markdown(ghi_chu)
        st.dataframe(styled_data)

    # Pivot the data to create a matrix for the heatmap

    with st.expander("TC Tiers"):
        ghi_chu2 = '''
        **[1] TC/ngày từ & TC/ngày đến**: khoảng TC/ngày của mỗi level  
        **[2] TC/tháng từ & TC/tháng đến**: khoảng TC/tháng của mỗi level tính theo :blue-background[30 ngày hoạt động]  
        **[3] Lương cơ bản tại Tier0/ngày & Lương cơ bản tại Tier0/tháng**: tính theo :blue-background[30 ngày hoạt động]  
        **[4] X-đơn giá tiền lương/TC** trong từng mức tier.          
        ***Ví dụ*** ở level **tier1**, có TC/ngày từ 51 đến 140, đơn giá X=40.000đ/TC thì giả sử tại ngày hoạt động có TC là 100, nhà hàng sẽ **:green[nhận thêm]** tiền lương tại mức tier1 là:  
        :money_with_wings: (100-51+1)*40.000 = **:green[2.000.000đ]**.  
        Với lương cơ bản tại tier0 = 1.800.000đ/ngày thì **lương khoán tại ngày hôm đó** sẽ là:  
        :moneybag: 1.800.000 + 2.000.000 = **:green[3.800.000đ]**  

        Vẫn ví dụ ở level tier1, có TC/tháng từ 1.501 đến 4.200, đơn giá vẫn là 40.000đ/TC thì giả sử cả tháng đạt 1.800TC, nhà hàng sẽ **:green[nhận thêm]** tiền lương tại mức tier1 là:  
        :money_with_wings: (1.800-1.501+1)*40.000 = **:green[12.000.000]**.  
        Với lương cơ bản tại tier0 = 54.000.000đ/tháng, tổng **lương khoán tại tháng đó** sẽ là:  
        :moneybag: 54.000.000 + 12.000.000 = **:green[66.000.000đ]**
        '''
        st.markdown(ghi_chu2)
        styled_tctier = display_tiertc(tier_tc)
        st.dataframe(styled_tctier)

    with st.container(border=True):
        st.plotly_chart(fig2)