using BLL.Services;
using DAL;
using DAL.Persistence;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddLogging();

// EF Core / PostgreSQL (local)
var pgConnectionString = "Host=localhost;Port=5432;Database=mgravel_db;Username=postgres;Password=123456";
builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(pgConnectionString));


// JanusGraph / Gremlin client setup
builder.Services.AddJanusGraph(o =>
{
    o.Host = "localhost";    // Gremlin Server host
    o.Port = 8182;            // Gremlin Server port
    o.EnableSsl = false;      // Set true if your Gremlin Server uses wss
    // o.Username = "";      // If auth is enabled on Gremlin Server
    // o.Password = "";
    o.PoolSize = 16;          // Connection pool size for high-ingest
    o.MaxInProcessPerConnection = 64;
});

// BLL Services
builder.Services.AddSingleton<FhirValidationService>(); // Stateless, only loads schema once
builder.Services.AddScoped<FhirReferenceService>();
builder.Services.AddScoped<FhirPersistenceService>();
builder.Services.AddScoped<FhirConditionalService>();
builder.Services.AddScoped<FhirVersioningService>();
builder.Services.AddScoped<FhirBundleService>();
builder.Services.AddScoped<FhirPatientService>();
builder.Services.AddScoped<FHIRService>();
builder.Services.AddScoped<GraphOpsService>();
builder.Services.AddScoped<TestOpsService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
