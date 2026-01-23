using DAL;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();


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

// FHIR service
builder.Services.AddScoped<BLL.Services.FHIRService>();

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
