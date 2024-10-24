import streamlit as st
import duckdb
import pandas as pd
import datetime
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
st.set_page_config(layout="wide")

# data folder path
cwd = Path(__file__).parent

# daily file
dta_daily_path = cwd.joinpath('data_luongtt.parquet')

db = duckdb.connect()
db.execute(f"CREATE or replace temp VIEW data_daily AS SELECT * FROM '{dta_daily_path}'")

st.title("Dashboard Lương khoán")

@st.cache_data
def get_data_daily(store=''):
    if len(store)>0:
        query = rf'''
        SELECT 
            *
        FROM data_daily 
        WHERE store_vt in ({store})
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

def chart_luong_tt(data_daily):
    '''
    Chat the hien chenh lech giua Luong TT thuc te voi Luong khoan tinh theo moving avg TC MTD
    '''
    # Define colors for 'chenh_lech_luong_khoan'
    colors = ['#87FE1A' if val > 0 else '#F90202' for val in data_daily['chenh_lech_luong_khoan']]

    # Create a stacked bar chart using Plotly
    fig = go.Figure()

    # Lollipop chart for 'luong_tt_daily_avg_moving_avg_mtd' and 'total_luongtt_act'
    for idx, row in data_daily.iterrows():
        # Line from 'total_luongtt_act' to 'luong_tt_daily_avg_moving_avg_mtd'
        fig.add_trace(go.Scatter(
            x=[row['report_date'], row['report_date']],
            y=[0, row['luong_tt_daily_avg_moving_avg_mtd']],
            mode='lines',
            line=dict(color='grey', width=0.5),
            showlegend=False
        ))
        # Line from 'total_luongtt_act' to 'luong_tt_daily_avg_moving_avg_mtd'
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
        # Point for 'luong_tt_daily_avg_moving_avg_mtd'
        fig.add_trace(go.Scatter(
            x=[row['report_date']],
            y=[row['luong_tt_daily_avg_moving_avg_mtd']],
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
        customdata=data_daily[['total_luongtt_act', 'luong_tt_daily_avg_moving_avg_mtd', 'chenh_lech_luong_khoan']]
    ))

    # Update layout
    fig.update_layout(
        title='Chênh lệch lương khoán',
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
        name='Baseline RFC',
        line_shape='hvh',
        line=dict(color='red', width=2)
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
    color = 'background-color: #FF5353' if row['chenh_lech_luong_khoan_avg'] < 0 else (
            'background-color: #67FF53' if row['chenh_lech_luong_khoan_avg'] > 0 else '')
    return [color] * len(row)

# Define a function to apply text color based on 'chenh_lech_luong_khoan'
def highlight_text(row):
    color = '#FF5353' if row['Chênh lệch lương khoán'] < 0 else (
            '#3CB32D' if row['Chênh lệch lương khoán'] > 0 else 'black')
    return [f'color: {color}' for _ in row]

def display_table(data_daily):
    # Store original numeric values for formatting and display
    
    cols = ['brand', 'store_vt', 'tc_forecast', 'report_date', 'tc', 
            'luongtt_gstar','luongtt_ggg','total_luongtt_act', 
            # 'luong_tt_daily_avg_mtd', 'mtd_avg_tc', 'chenh_lech_luong_khoan_avg',
            'luong_tt_daily_avg_moving_avg_mtd', 'moving_mtd_avg_tc', 'chenh_lech_luong_khoan',
        #    'tc_from_daily_mtd', 'tc_to_daily_mtd', 
        #    'luong_tt_tier0',
        #    'bonus_per_tc_over_avg_mtd', 'bonus_fix_daily_avg_mtd',       'bonus_daily_avg_mtd', 
        'whr_sche','whr_act', 
        'baseline_act', 'baseline_rfc', 'whr_gstar', 
        'total_whr_act']
    data_table = data_daily[cols]

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
                # 'luong_tt_daily_avg_mtd':'Lương khoán', 
                # 'mtd_avg_tc':'TC trung bình', 
                # 'chenh_lech_luong_khoan_avg':'Chênh lệch lương khoán',
                'luong_tt_daily_avg_moving_avg_mtd':'Lương khoán', 
                'moving_mtd_avg_tc':'TC trung bình', 
                'chenh_lech_luong_khoan':'Chênh lệch lương khoán',
                # 'tc_from_daily_mtd', 'tc_to_daily_mtd', 'luong_tt_tier0',
                # 'bonus_per_tc_over_avg_mtd', 'bonus_fix_daily_avg_mtd',
                # 'bonus_daily_avg_mtd', 
                'whr_sche':'Giờ công lập lịch',
                'baseline_act':'Baseline Actual', 
                'baseline_rfc':'Baseline Forecast', 
                'whr_act':'Giờ công thực tế', 
                'whr_gstar':'Giờ công Gstar', 
                'total_whr_act':'Tổng giờ công'
    }

    format_cols2 =['TC Forecast', 'TC Actual', 'Lương Gstar', 'Lương trực tiếp','Tổng lương trực tiếp','Lương khoán','Chênh lệch lương khoán','TC trung bình','Giờ công lập lịch','Baseline Actual','Baseline Forecast','Giờ công thực tế', 'Giờ công Gstar', 'Tổng giờ công' ]
    data_display.rename(columns=rename_cols, inplace=True)
    # )
    styled_data = data_display.style.format(subset=format_cols2, formatter="{:,.0f}").apply(highlight_text, axis=1)
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
                    'Lương khoán': ':,.0f', 
                    'TC forecast':':,.0f', 
                    'TC Actual':':,.0f', 
                    },
                    )
    # Update layout for better visualization (optional)
    fig1.update_traces(marker=dict(opacity=0.7),
                    meanline_visible=True,
                    )  # Adjust marker opacity if needed
    fig1.update_layout(
        title='Chênh lệch lương khoán theo ngày trong tuần',
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
                    'Lương khoán': ':,.0f', 
                    'TC forecast':':,.0f', 
                    'TC Actual':':,.0f', 
                    },
                    )
    # Update layout for better visualization (optional)
    fig2.update_traces(marker=dict(opacity=0.7),
                    meanline_visible=True,
                    )  # Adjust marker opacity if needed
    fig2.update_layout(
        title='Chênh lệch lương khoán theo từng nhà',
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
    # from_date = st.sidebar.date_input('Lay du lieu tu ngay', value=start_of_month)
    # to_date = st.sidebar.date_input('Lay du lieu den ngay')


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


    data_daily = get_data_daily(chon_store)
    # st.dataframe(data_daily)

    data_daily['chenh_lech_luong_khoan'] = data_daily['luong_tt_daily_avg_moving_avg_mtd'] - data_daily['total_luongtt_act']
    data_daily['chenh_lech_luong_khoan_avg'] = data_daily['luong_tt_daily_avg_mtd'] - data_daily['total_luongtt_act']
    data_daily['min_luong'] = data_daily[['luong_tt_daily_avg_moving_avg_mtd', 'total_luongtt_act']].min(axis=1)
    data_daily['abs_chenh_lech'] = data_daily['chenh_lech_luong_khoan'].abs()
    data_daily['pct_whr_gstar_to_baseline'] = (data_daily['whr_gstar'] / data_daily['baseline_rfc']) * 100
    data_daily['pct_whr_gstar_to_total_whr'] = (data_daily['whr_gstar'] / data_daily['total_whr_act']) * 100

    format_cols = [
        'tc', 'mtd_avg_tc', 'tc_from_daily_mtd', 'tc_to_daily_mtd', 'luong_tt_tier0',
        'bonus_per_tc_over_avg_mtd', 'bonus_fix_daily_avg_mtd', 'bonus_daily_avg_mtd',
        'luong_tt_daily_avg_mtd', 'whr_act', 'whr_sche', 'baseline_act', 'baseline_rfc',
        'whr_gstar', 'luongtt_gstar', 'total_whr_act','chenh_lech_luong_khoan_avg',
        'luongtt_ggg', 'total_luongtt_act'
    ]

    for col in format_cols:
        data_daily[col] = pd.to_numeric(data_daily[col])



    # st.dataframe(data_daily[cols])


    # st.dataframe(data_daily.head())

    data_chart = data_daily.groupby(['report_date'], as_index=False).agg({'tc':'sum',
                                                                        'tc_forecast':'sum',
                                                                        'luong_tt_daily_avg_moving_avg_mtd':'sum',
                                                                        'total_luongtt_act':'sum',
                                                                        'chenh_lech_luong_khoan':'sum',
                                                                        'abs_chenh_lech':'sum',
                                                                        'min_luong':'sum',
                                                                        'baseline_rfc':'sum',
                                                                        'whr_act':'sum',
                                                                        'whr_gstar':'sum',
                                                                        'total_whr_act':'sum',
                                                                        

                                                                        })

    chart_luongtt = chart_luong_tt(data_chart)
    chart_tc = chart_tc(data_chart)


    chart_whr = chart_whr(data_chart)

    tong_khoan = data_daily['luong_tt_daily_avg_mtd'].sum()
    tong_actual = data_daily['total_luongtt_act'].sum()
    chenh_lech = tong_khoan - tong_actual


    box_data = data_daily.rename(columns={
                    'report_date': 'Date',
                    'store_vt':'Store', 
                    'chenh_lech_luong_khoan': 'Chênh lệch',  # Format with comma and one decimal place
                    'total_luongtt_act': 'Lương thực tế', 
                    'luong_tt_daily_avg_moving_avg_mtd': 'Lương khoán', 
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
                st.metric(label='Lương khoán tạm tính', 
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



    with st.expander("Dữ liệu chi tiết"):
    # Display the formatted DataFrame
        styled_data=display_table(data_daily)
        st.dataframe(styled_data)

    # Pivot the data to create a matrix for the heatmap

    with st.container(border=True):
        st.plotly_chart(fig2)