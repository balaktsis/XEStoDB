clc
clear
close all

load('event_insertion_times.mat')


figure("Name","SEPSIS Log - Event insertion times");
subplot(2,2,1)
plotInsTimes(mono_sepsis, 1)
title('Monolithic schema')
set(gca, 'fontsize',12)
xlabel('Event position')
ylabel('Time (ms)')

lgd = legend("Event ins. times", "Trend-line");

subplot(2,2,2)
plotInsTimes(dbxes_sepsis, 1)
title('DBXES schema')
set(gca, 'fontsize',12)
xlabel('Event position')
ylabel('Time (ms)')

subplot(2,2,3)
plotInsTimes(rxes_sepsis, 1)
title('RXES schema')
set(gca, 'fontsize',12)
xlabel('Event position')
ylabel('Time (ms)')

subplot(2,2,4)
plotInsTimes(rxesPlus_sepsis, 1)
title('RXES+ schema')
set(gca, 'fontsize',12)
xlabel('Event position')
ylabel('Time (ms)')

function plotInsTimes(matrix, trendline_degree)
    avg = trimmean(matrix, 40, 2);
    evt_num = numel(avg);
    x_axis = linspace(1, evt_num, evt_num);
    
    coeffs = polyfit(x_axis, avg, trendline_degree);
    trendline = polyval(coeffs, x_axis);
    
    stem(x_axis, avg, 'Marker','x')
    xlim([1 evt_num])
    hold on
    plot(x_axis, trendline, 'LineWidth',5)
end