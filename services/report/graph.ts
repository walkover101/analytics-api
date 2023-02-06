import { Chart, BarController, PieController, ArcElement, BarElement, LineController, CategoryScale, LinearScale, PointElement, LineElement, DoughnutController, Title, Legend } from 'chart.js';
Chart.register(LineController);
Chart.register(BarController);
Chart.register(DoughnutController);
Chart.register(PieController);
Chart.register(ArcElement);
Chart.register(BarElement);
Chart.register(CategoryScale);
Chart.register(LinearScale);
Chart.register(PointElement);
Chart.register(LineElement);
Chart.register(Title);
Chart.register(Legend);
import { createCanvas } from 'canvas';

export function createBarChart(labels: string[], values: number[], option: { width: number, height: number }) {
    const canvas = createCanvas(option?.width, option?.height);
    const ctx = canvas.getContext('2d');
    const data = {
        labels: labels,
        datasets: [
            {
                label: 'My First Dataset',
                backgroundColor: "#5996c9",
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1,
                data: values,
            },
        ],
    };

    const options = {};

    const myChart = new Chart(ctx as any, {
        type: 'bar',
        data,
        options,
    });
    return myChart.toBase64Image('image/png', 1.0);
    // const writeStream = fs.createWriteStream("chart.png", { flags: 'w' });
    // canvas.createPNGStream().pipe(writeStream);
    // canvas.createPNGStream().
}


export function createPieChart(labels: string[], values: number[], option: { width: number, height: number }) {
    const canvas = createCanvas(option?.width, option?.height);
    const ctx = canvas.getContext('2d');
    const data = {
        labels: labels,
        datasets: [
            {
                label: 'My First Dataset',
                backgroundColor: ["#4085c0","#b5aee3","#8599d3","#d3d3d3","#f2b5d3","#f2d3b5","#f2f2b5"],
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1,
                data: values,
            },
        ],
    };

    const options = {};

    const myChart = new Chart(ctx as any, {
        type: 'pie',
        data,
        options,
    });
    return myChart.toBase64Image('image/png', 1.0);
}

export function createDoughnutChart(labels: string[], values: number[], option: { width: number, height: number }) {
    const canvas = createCanvas(option?.width, option?.height);
    const ctx = canvas.getContext('2d');
    const data = {
        labels: labels,
        datasets: [
            {
                label: 'My First Dataset',
                backgroundColor: ["#4085c0","#b5aee3","#8599d3","#d3d3d3","#f2b5d3","#f2d3b5","#f2f2b5"],
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1,
                data: values,
            },
        ],
    };

    const options = {};

    const myChart = new Chart(ctx as any, {
        type: 'doughnut',
        data,
        options,
    });
    return myChart.toBase64Image('image/png', 1.0);
}
